"""
Container Lifecycle Event Producer
Reads real staging data and publishes to Kafka topics
Supports: gate events, cleaning, M&R, yard movements, and inspections

Simulation Mode (default for --mode loop):
  - Uses a SimulationClock to advance time day-by-day through historical data
  - Merges ALL event sources into one globally-sorted chronological stream
  - Preserves original source event_time unchanged in every published event
  - State is persisted in /app/state/sim_clock.json for crash-safe restarts
  - On exhausting the data window the clock wraps back to start automatically
"""
import json
import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _safe(val, default: str = '') -> str:
    """Convert a value to string, returning default for NaN/None."""
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return str(val)


def _norm_ts(val) -> str:
    """
    Normalize a timestamp value to "yyyy-MM-ddTHH:MM:SS.ffffff" (microseconds,
    no timezone offset) so Silver's parse_event_timestamp can reliably parse it.

    Handles:
    - pandas Timestamp / numpy datetime64 (nanosecond or microsecond precision)
    - raw strings from CSV: "2026-01-19 06:23:22.575854069" (nanosecond)
    - datetime.datetime objects
    Falls back to str(val) if parsing fails.
    """
    if val is None:
        return ''
    try:
        if pd.isna(val):
            return ''
    except (TypeError, ValueError):
        pass
    try:
        ts = pd.to_datetime(val)
        return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")   # microsecond, no tz
    except Exception:
        return str(val)


# ---------------------------------------------------------------------------
# SimulationClock – manages day-by-day replay state
# ---------------------------------------------------------------------------

class SimulationClock:
    """
    Persists simulation progress to disk so the producer can resume after
    a container restart.  Replays events in strict chronological order,
    preserving original source event_time unchanged.
    """

    STATE_FILE = "/app/state/sim_clock.json"

    def __init__(self, data_start: Optional[datetime] = None,
                 data_end: Optional[datetime] = None):
        self.state = self._load_or_init(data_start, data_end)

    # -- persistence -----------------------------------------------------------

    def _load_or_init(self, data_start: Optional[datetime],
                      data_end: Optional[datetime]) -> dict:
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE) as f:
                state = json.load(f)
            logger.info(
                f"[SimClock] Resumed: sim_date={state['sim_date']}  "
                f"total_published={state.get('total_published', 0)}"
            )
            return state

        if data_start is None or data_end is None:
            raise ValueError("data_start and data_end required for first run")

        state = {
            "sim_date":        data_start.strftime("%Y-%m-%d"),
            "data_start":      data_start.isoformat(),
            "data_end":        data_end.isoformat(),
            "real_start":      datetime.utcnow().isoformat(),
            "total_published": 0,
        }
        self._save(state)
        logger.info(
            f"[SimClock] Initialized. Data range: "
            f"{data_start.date()} → {data_end.date()}"
        )
        return state

    def _save(self, state: Optional[dict] = None) -> None:
        os.makedirs(os.path.dirname(self.STATE_FILE), exist_ok=True)
        with open(self.STATE_FILE, "w") as f:
            json.dump(state or self.state, f, default=str, indent=2)

    def reset(self) -> None:
        """Delete persisted state so next run re-initialises from scratch."""
        if os.path.exists(self.STATE_FILE):
            os.remove(self.STATE_FILE)
            logger.info("[SimClock] State reset.")

    # -- accessors -------------------------------------------------------------

    @property
    def sim_date(self) -> datetime:
        return datetime.strptime(self.state["sim_date"], "%Y-%m-%d")

    @property
    def data_start(self) -> datetime:
        return datetime.fromisoformat(self.state["data_start"])

    @property
    def data_end(self) -> datetime:
        return datetime.fromisoformat(self.state["data_end"])

    def current_window(self, advance_days: int = 1) -> Tuple[datetime, datetime]:
        """Returns (start_inclusive, end_exclusive) for the current sim day."""
        start = self.sim_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=advance_days)
        return start, end

    # -- simulation advance ----------------------------------------------------

    def advance(self, days: int = 1) -> datetime:
        """Advance the clock by *days*.  Wraps back to data_start when the end of the dataset is reached."""
        new_sim = self.sim_date + timedelta(days=days)
        if new_sim > self.data_end:
            new_sim = self.data_start
            logger.info("[SimClock] Dataset fully replayed – wrapping to start.")
        self.state["sim_date"] = new_sim.strftime("%Y-%m-%d")
        self._save()
        logger.info(f"[SimClock] Advanced → {new_sim.date()}")
        return new_sim

    def record_published(self, count: int) -> None:
        self.state["total_published"] = self.state.get("total_published", 0) + count
        self._save()


class ContainerEventProducer:
    """Producer for container lifecycle events"""

    @staticmethod
    def _extract_ct_facility(value: str) -> str:
        """Extract terminal facility code (CT01/CT02/...) from a raw string."""
        if value is None:
            return "UNKNOWN"
        s = str(value).strip()
        m = re.search(r"(CT\d{2})", s)
        return m.group(1) if m else "UNKNOWN"

    @staticmethod
    def _normalize_mnr_stage(value: str) -> str:
        """Normalize raw MNR stage variants to canonical stage labels."""
        stage = str(value or "").strip().upper()
        if stage == "APPROVAL":
            return "APPROVED"
        if stage == "COMPLETED":
            return "REPAIRED"
        return stage

    @staticmethod
    def _derive_mnr_event_type(stage_value: str) -> str:
        """Derive producer event_type from stage so CSV event_type is optional."""
        stage = ContainerEventProducer._normalize_mnr_stage(stage_value)
        if stage == "REPAIRED":
            return "MNR_REPAIRED"
        if stage == "APPROVED":
            return "MNR_APPROVED"
        return "MNR_RECEIVED"

    
    # Map event types to Kafka topics
    TOPIC_MAPPING = {
        # Gate
        'GATE_IN': 'raw.gate',
        'GATE_OUT': 'raw.gate',

        # Yard moves
        'yard_move': 'raw.yard_move',
        'YARD_MOVE': 'raw.yard_move',

        # Inspections
        'inspection': 'raw.inspection',
        'INSPECTION': 'raw.inspection',

        # Cleaning
        'CLEANING': 'raw.cleaning',
        'CLEAN': 'raw.cleaning',
        'WASHING': 'raw.cleaning',

        # M&R (support synthetic + real variants)
        'MNR_ESTIMATE': 'raw.mnr',
        'MNR_EST': 'raw.mnr',
        'MNR_APPROVAL': 'raw.mnr',
        'MNR_APPROVED': 'raw.mnr',
        'MNR_APPR': 'raw.mnr',
        'MNR_COMPLETE': 'raw.mnr',
        'MNR_COMP': 'raw.mnr',
        'MNR_RECEIVED': 'raw.mnr',
        'MNR_REPAIRED': 'raw.mnr'
    }

    # Data file mappings
    DATA_FILES = {
        'gate': '/app/data/stg_gate_events.csv',
        'cleaning': '/app/data/stg_cleaning_events.csv',
        'mnr': '/app/data/stg_mnr_events.csv',
        'yard_move': '/app/data/yard_location_movement.csv',
        'inspection': '/app/data/inspection_damage_report.csv'
    }
    
    def __init__(self, bootstrap_servers: str = 'kafka:9092'):
        """Initialize Kafka producer"""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Connect to Kafka"""
        retry_count = 0
        max_retries = 10
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except KafkaError as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka after maximum retries")
    
    def publish_event(self, topic: str, event: Dict, key: str = None):
        """Publish a single event to Kafka"""
        try:
            # Add metadata
            now = datetime.utcnow()
            event['ingest_time'] = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}"
            
            future = self.producer.send(topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False
    
    def load_csv_data(self, filepath: str) -> pd.DataFrame:
        """Load data from CSV file"""
        try:
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} records from {filepath}")
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return pd.DataFrame()
    
    def publish_gate_events(self, filepath: str, limit: int = None):
        """Publish gate events from stg_gate_events.csv"""
        df = self.load_csv_data(filepath)
        if df.empty:
            return 0
        
        if limit:
            df = df.head(limit)
        
        published_count = 0
        for idx, row in df.iterrows():
            event_type = str(row.get('event_type', '')).strip().upper()  # normalize: gate_out → GATE_OUT
            if not event_type or event_type not in self.TOPIC_MAPPING:
                continue
            
            topic = self.TOPIC_MAPPING[event_type]
            container_no = str(row.get('container_no_raw', ''))
            position_value = str(row.get('position', ''))
            location_value = str(row.get('location', ''))
            facility_value = self._extract_ct_facility(location_value)
            if facility_value == "UNKNOWN":
                facility_value = self._extract_ct_facility(position_value)
            
            event = {
                'event_id': f"GATE_{str(row.get('source_file', os.path.basename(filepath)))}_{str(row.get('source_row', idx))}",
                'event_type': event_type,
                'event_time': _norm_ts(row.get('event_time', '')),
                'container_no_raw': container_no,
                'eir': str(row.get('eir', '')),
                'seq': str(row.get('seq', '')),
                'type_raw': str(row.get('type_raw', '')),
                'opt': str(row.get('opt', '')),
                'move': str(row.get('move', '')),
                'booking': str(row.get('booking', '')),
                'truck': str(row.get('truck', '')),
                'vessel': str(row.get('vessel', '')),
                'voyage': str(row.get('voyage', '')),
                'dest': str(row.get('dest', '')),
                'grade': str(row.get('grade', '')),
                'position': position_value,
                'location': location_value,
                'remark': str(row.get('remark', '')),
                'nominate_remark': str(row.get('nominate_remark', '')),
                'facility': facility_value,
                'source_file': str(row.get('source_file', os.path.basename(filepath))),
                'source_sheet': str(row.get('source_sheet', '')),
                'source_row': str(row.get('source_row', idx)),
                'is_synthetic': str(row.get('is_synthetic', '0'))
            }
            
            if self.publish_event(topic, event, container_no):
                published_count += 1
            time.sleep(0.005)
        
        logger.info(f"Published {published_count} gate events")
        return published_count
    
    def publish_cleaning_events(self, filepath: str, limit: int = None):
        """Publish cleaning events from stg_cleaning_events.csv"""
        df = self.load_csv_data(filepath)
        if df.empty:
            return 0
        
        if limit:
            df = df.head(limit)
        
        published_count = 0
        for idx, row in df.iterrows():
            event_type = str(row.get('event_type', 'CLEANING')).strip()
            if event_type not in self.TOPIC_MAPPING:
                event_type = 'CLEANING'
            
            topic = self.TOPIC_MAPPING.get(event_type, 'raw.cleaning')
            container_no = str(row.get('container_no_raw', ''))
            
            event = {
                'event_id': f"CLEAN_{str(row.get('source_file', os.path.basename(filepath)))}_{str(row.get('source_row', idx))}",
                'event_type': event_type,
                'event_time': _norm_ts(row.get('event_time', '')),
                'container_no_raw': container_no,
                'type_raw': str(row.get('type_raw', '')),
                'remark_raw': str(row.get('remark_raw', '')),
                'amount': str(row.get('cost', row.get('amount', ''))),
                'currency': str(row.get('currency', 'USD')),
                'facility': self._extract_ct_facility(str(row.get('facility', ''))),
                'source_file': str(row.get('source_file', os.path.basename(filepath))),
                'source_sheet': str(row.get('source_sheet', '')),
                'source_row': str(row.get('source_row', idx)),
                'is_synthetic': str(row.get('is_synthetic', '0'))
            }

            if self.publish_event(topic, event, container_no):
                published_count += 1
            time.sleep(0.005)
        
        logger.info(f"Published {published_count} cleaning events")
        return published_count
    
    def publish_mnr_events(self, filepath: str, limit: int = None):
        """Publish M&R events from stg_mnr_events.csv"""
        df = self.load_csv_data(filepath)
        if df.empty:
            return 0
        
        if limit:
            df = df.head(limit)
        
        published_count = 0
        for idx, row in df.iterrows():
            stage_value = str(row.get('stage', ''))
            event_type = self._derive_mnr_event_type(stage_value)
            topic = 'raw.mnr'
            container_no = str(row.get('container_no_raw', ''))
            
            event = {
                'event_id': f"MNR_{str(row.get('source_file', os.path.basename(filepath)))}_{str(row.get('source_row', idx))}",
                'event_type': event_type,
                'event_time': _norm_ts(row.get('event_time', '')),
                'container_no_raw': container_no,
                'size_raw': str(row.get('size_raw', '')),
                'location_raw': str(row.get('location_raw', '')),
                'note_raw': str(row.get('note_raw', '')),
                'stage': self._normalize_mnr_stage(stage_value),
                'facility': self._extract_ct_facility(str(row.get('location_raw', ''))),
                'source_file': str(row.get('source_file', os.path.basename(filepath))),
                'source_sheet': str(row.get('source_sheet', '')),
                'source_row': str(row.get('source_row', idx)),
                'is_synthetic': str(row.get('is_synthetic', '0'))
            }
            
            if self.publish_event(topic, event, container_no):
                published_count += 1
            time.sleep(0.005)
        
        logger.info(f"Published {published_count} M&R events")
        return published_count
    
    def publish_yard_move_events(self, filepath: str, limit: int = None):
        """Publish yard movement events from yard_location_movement.csv"""
        df = self.load_csv_data(filepath)
        if df.empty:
            return 0
        
        if limit:
            df = df.head(limit)
        
        published_count = 0
        for idx, row in df.iterrows():
            container_no = str(row.get('container_no_raw', ''))
            
            from_location = f"{_safe(row.get('from_block'))}-{_safe(row.get('from_row'))}-{_safe(row.get('from_bay'))}-{_safe(row.get('from_tier'))}"
            to_location = f"{_safe(row.get('to_block'))}-{_safe(row.get('to_row'))}-{_safe(row.get('to_bay'))}-{_safe(row.get('to_tier'))}"
            
            stable_id = str(row.get('move_id', str(idx)))
            event = {
                'event_id': stable_id,
                'event_type': 'yard_move',
                'event_time': _norm_ts(row.get('move_time', '')),
                'container_no_raw': container_no,
                'facility': str(row.get('facility', 'UNKNOWN')),
                'from_location': from_location,
                'from_block': str(row.get('from_block', '')),
                'from_row': str(row.get('from_row', '')),
                'from_bay': str(row.get('from_bay', '')),
                'from_tier': str(row.get('from_tier', '')),
                'to_location': to_location,
                'to_block': str(row.get('to_block', '')),
                'to_row': str(row.get('to_row', '')),
                'to_bay': str(row.get('to_bay', '')),
                'to_tier': str(row.get('to_tier', '')),
                'move_reason': str(row.get('move_reason', '')),
                'equipment_id': str(row.get('equipment_id', '')),
                'operator_id': str(row.get('operator_id', '')),
                'source_file': os.path.basename(filepath),
                'source_row': stable_id,  # stable CSV-origin key for Silver dedup
            }
            
            if self.publish_event('raw.yard_move', event, container_no):
                published_count += 1
            time.sleep(0.005)
        
        logger.info(f"Published {published_count} yard move events")
        return published_count
    
    def publish_inspection_events(self, filepath: str, limit: int = None):
        """Publish inspection events from inspection_damage_report.csv"""
        df = self.load_csv_data(filepath)
        if df.empty:
            return 0
        
        if limit:
            df = df.head(limit)
        
        published_count = 0
        for idx, row in df.iterrows():
            container_no = str(row.get('container_no_raw', ''))
            
            stable_id = str(row.get('inspection_id', str(idx)))
            event = {
                'event_id': stable_id,
                'event_type': 'inspection',
                'event_time': _norm_ts(row.get('inspection_time', '')),
                'container_no_raw': container_no,
                'facility': str(row.get('facility', 'UNKNOWN')),
                'damage_code': str(row.get('damage_code', '')),
                'component': str(row.get('component', '')),
                'severity': _safe(row.get('severity', '')),  # _safe returns '' for pandas NaN → Silver maps '' → NULL (not NO_DEFECT)
                'estimated_cost': str(row.get('estimated_cost', '')),
                'currency': str(row.get('currency', '')),
                'inspector_id': str(row.get('inspector_id', '')),
                'photo_ref': str(row.get('photo_ref', '')),
                'remarks': str(row.get('remarks', '')),
                'source': str(row.get('source', os.path.basename(filepath))),
                'source_file': os.path.basename(filepath),  # stable CSV-origin key for Silver dedup
                'source_row': stable_id,
            }
            
            if self.publish_event('raw.inspection', event, container_no):
                published_count += 1
            time.sleep(0.005)
        
        logger.info(f"Published {published_count} inspection events")
        return published_count
    
    def publish_all_events(self, limit_per_type: int = None):
        """Publish all event types from their respective files"""
        total_published = 0
        
        logger.info("Publishing gate events...")
        total_published += self.publish_gate_events(self.DATA_FILES['gate'], limit_per_type)
        
        logger.info("Publishing cleaning events...")
        total_published += self.publish_cleaning_events(self.DATA_FILES['cleaning'], limit_per_type)
        
        logger.info("Publishing M&R events...")
        total_published += self.publish_mnr_events(self.DATA_FILES['mnr'], limit_per_type)
        
        logger.info("Publishing yard move events...")
        total_published += self.publish_yard_move_events(self.DATA_FILES['yard_move'], limit_per_type)
        
        logger.info("Publishing inspection events...")
        total_published += self.publish_inspection_events(self.DATA_FILES['inspection'], limit_per_type)
        
        logger.info(f"Total published: {total_published} events")
        return total_published

    # -----------------------------------------------------------------------
    # Chronological / Simulation helpers
    # -----------------------------------------------------------------------

    def load_all_events_sorted(self) -> pd.DataFrame:
        """
        Load every event source, normalise the timestamp field to a common
        column ``_event_time`` (datetime), then return a single DataFrame
        sorted ascending by that column.

        This is the foundation of the simulation mode: all events from all
        topics form one unified, chronologically ordered stream so that Spark
        always sees GATE_IN before GATE_OUT, MNR_RECEIVED before MNR_REPAIRED,
        etc., for every container.
        """
        frames = []

        # Gate
        df = self.load_csv_data(self.DATA_FILES['gate'])
        if not df.empty:
            df['_source_type'] = 'gate'
            df['_event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
            frames.append(df)

        # Cleaning
        df = self.load_csv_data(self.DATA_FILES['cleaning'])
        if not df.empty:
            df['_source_type'] = 'cleaning'
            df['_event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
            frames.append(df)

        # M&R
        df = self.load_csv_data(self.DATA_FILES['mnr'])
        if not df.empty:
            df['_source_type'] = 'mnr'
            df['_event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
            frames.append(df)

        # Yard moves  (column is move_time, not event_time)
        df = self.load_csv_data(self.DATA_FILES['yard_move'])
        if not df.empty:
            df['_source_type'] = 'yard_move'
            ts_col = df.get('move_time', df.get('event_time', pd.Series()))
            df['_event_time'] = pd.to_datetime(ts_col, errors='coerce')
            frames.append(df)

        # Inspections  (column is inspection_time)
        df = self.load_csv_data(self.DATA_FILES['inspection'])
        if not df.empty:
            df['_source_type'] = 'inspection'
            ts_col = df.get('inspection_time', df.get('event_time', pd.Series()))
            df['_event_time'] = pd.to_datetime(ts_col, errors='coerce')
            frames.append(df)

        if not frames:
            logger.warning("No event data loaded – all files empty or missing.")
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined.dropna(subset=['_event_time'], inplace=True)
        combined.sort_values('_event_time', kind='mergesort', inplace=True)
        combined.reset_index(drop=True, inplace=True)

        logger.info(
            f"[ChronLoader] Merged {len(combined)} events | "
            f"range: {combined['_event_time'].min()} → {combined['_event_time'].max()}"
        )
        return combined

    def _build_event_from_row(self, row: pd.Series, idx: int,
                              event_time_str: str) -> Tuple[str, dict, str]:
        """
        Build a (topic, event_dict, partition_key) tuple from a unified row.
        ``event_time_str`` is the original source event_time ISO string,
        preserved unchanged from the source CSV data.
        """
        source_type = row.get('_source_type', 'gate')
        container_no = str(row.get('container_no_raw', ''))

        if source_type == 'gate':
            event_type = str(row.get('event_type', 'GATE_IN')).strip().upper()  # normalize: gate_out → GATE_OUT
            if event_type not in self.TOPIC_MAPPING:
                event_type = 'GATE_IN'
            topic = self.TOPIC_MAPPING[event_type]
            event = {
                'event_id':          f"GATE_{str(row.get('source_file', 'gate'))}_{str(row.get('source_row', idx))}",
                'event_type':        event_type,
                'event_time':        event_time_str,
                'container_no_raw':  container_no,
                'eir':               str(row.get('eir', '')),
                'seq':               str(row.get('seq', '')),
                'type_raw':          str(row.get('type_raw', '')),
                'opt':               str(row.get('opt', '')),
                'move':              str(row.get('move', '')),
                'booking':           str(row.get('booking', '')),
                'truck':             str(row.get('truck', '')),
                'vessel':            str(row.get('vessel', '')),
                'voyage':            str(row.get('voyage', '')),
                'dest':              str(row.get('dest', '')),
                'grade':             str(row.get('grade', '')),
                'position':          str(row.get('position', '')),
                'location':          str(row.get('location', '')),
                'remark':            str(row.get('remark', '')),
                'nominate_remark':   str(row.get('nominate_remark', '')),
                'facility':          self._extract_ct_facility(str(row.get('location', ''))),
                'source_file':       str(row.get('source_file', 'gate')),
                'source_sheet':      str(row.get('source_sheet', '')),
                'source_row':        str(row.get('source_row', idx)),
                'is_synthetic':      str(row.get('is_synthetic', '0')),
            }

        elif source_type == 'cleaning':
            event_type = str(row.get('event_type', 'CLEANING')).strip()
            if event_type not in self.TOPIC_MAPPING:
                event_type = 'CLEANING'
            topic = self.TOPIC_MAPPING.get(event_type, 'raw.cleaning')
            event = {
                'event_id':         f"CLEAN_{str(row.get('source_file', 'cleaning'))}_{str(row.get('source_row', idx))}",
                'event_type':       event_type,
                'event_time':       event_time_str,
                'container_no_raw': container_no,
                'type_raw':         str(row.get('type_raw', '')),
                'remark_raw':       str(row.get('remark_raw', '')),
                'amount':           str(row.get('cost', row.get('amount', ''))),
                'currency':         str(row.get('currency', 'USD')),
                'facility':         self._extract_ct_facility(str(row.get('facility', ''))),
                'source_file':      str(row.get('source_file', 'cleaning')),
                'source_sheet':     str(row.get('source_sheet', '')),
                'source_row':       str(row.get('source_row', idx)),
                'is_synthetic':     str(row.get('is_synthetic', '0')),
            }
            # topic already set above via TOPIC_MAPPING lookup — no second assignment needed

        elif source_type == 'mnr':
            stage_value = str(row.get('stage', ''))
            event_type = self._derive_mnr_event_type(stage_value)
            topic = 'raw.mnr'
            event = {
                'event_id':           f"MNR_{str(row.get('source_file', 'mnr'))}_{str(row.get('source_row', idx))}",
                'event_type':         event_type,
                'event_time':         event_time_str,
                'container_no_raw':   container_no,
                'size_raw':           str(row.get('size_raw', '')),
                'location_raw':       str(row.get('location_raw', '')),
                'note_raw':           str(row.get('note_raw', '')),
                'stage':              self._normalize_mnr_stage(stage_value),
                'facility':           self._extract_ct_facility(str(row.get('location_raw', ''))),
                'source_file':        str(row.get('source_file', 'mnr')),
                'source_sheet':       str(row.get('source_sheet', '')),
                'source_row':         str(row.get('source_row', idx)),
                'is_synthetic':       str(row.get('is_synthetic', '0')),
            }

        elif source_type == 'yard_move':
            from_location = (
                f"{_safe(row.get('from_block'))}-{_safe(row.get('from_row'))}"
                f"-{_safe(row.get('from_bay'))}-{_safe(row.get('from_tier'))}"
            )
            to_location = (
                f"{_safe(row.get('to_block'))}-{_safe(row.get('to_row'))}"
                f"-{_safe(row.get('to_bay'))}-{_safe(row.get('to_tier'))}"
            )
            topic = 'raw.yard_move'
            stable_id = str(row.get('move_id', str(idx)))
            event = {
                'event_id':         stable_id,
                'event_type':       'yard_move',
                'event_time':       event_time_str,
                'container_no_raw': container_no,
                'facility':         str(row.get('facility', 'UNKNOWN')),
                'from_location':    from_location,
                'from_block':       str(row.get('from_block', '')),
                'from_row':         str(row.get('from_row', '')),
                'from_bay':         str(row.get('from_bay', '')),
                'from_tier':        str(row.get('from_tier', '')),
                'to_location':      to_location,
                'to_block':         str(row.get('to_block', '')),
                'to_row':           str(row.get('to_row', '')),
                'to_bay':           str(row.get('to_bay', '')),
                'to_tier':          str(row.get('to_tier', '')),
                'move_reason':      str(row.get('move_reason', '')),
                'equipment_id':     str(row.get('equipment_id', '')),
                'operator_id':      str(row.get('operator_id', '')),
                'source_file':      str(row.get('source_file', 'yard_move')),
                'source_row':       stable_id,  # stable CSV-origin key for Silver dedup
                'is_synthetic':     str(row.get('is_synthetic', '0')),
            }

        else:  # inspection
            topic = 'raw.inspection'
            stable_id = str(row.get('inspection_id', str(idx)))
            event = {
                'event_id':        stable_id,
                'event_type':      'inspection',
                'event_time':      event_time_str,
                'container_no_raw': container_no,
                'facility':        str(row.get('facility', 'UNKNOWN')),
                'damage_code':     str(row.get('damage_code', '')),
                'component':       str(row.get('component', '')),
                'severity':        _safe(row.get('severity', '')),  # _safe → '' for NaN → Silver maps '' → NULL (not NO_DEFECT)
                'estimated_cost':  str(row.get('estimated_cost', '')),
                'currency':        str(row.get('currency', '')),
                'inspector_id':    str(row.get('inspector_id', '')),
                'photo_ref':       str(row.get('photo_ref', '')),
                'remarks':         str(row.get('remarks', '')),
                'source':          str(row.get('source', 'inspection')),
                'source_file':     str(row.get('source_file', 'inspection')),  # stable CSV-origin key
                'source_row':      stable_id,
                'is_synthetic':    str(row.get('is_synthetic', '0')),
            }

        return topic, event, container_no

    def publish_simulation_window(self, clock: SimulationClock,
                                  all_events: pd.DataFrame,
                                  advance_days: int = 1,
                                  inter_event_delay: float = 0.005) -> int:
        """
        Publish all events that fall within the current simulation window
        [sim_date, sim_date + advance_days) in strict chronological order.
        Each event preserves its original source event_time unchanged.
        After publishing, the simulation clock is advanced automatically.
        """
        win_start, win_end = clock.current_window(advance_days)
        mask = (all_events['_event_time'] >= win_start) & \
               (all_events['_event_time'] < win_end)
        window_df = all_events[mask]

        logger.info(
            f"[SimWindow] {win_start.date()} → {win_end.date()} | "
            f"{len(window_df)} events"
        )

        published = 0
        for idx, row in window_df.iterrows():
            # Emit without timezone offset and without nanoseconds so Silver's
            # parse_event_timestamp format "yyyy-MM-dd'T'HH:mm:ss.SSSSSS" matches
            # reliably.  isoformat() includes "+00:00" for UTC-aware timestamps and
            # may include 9 decimal places (nanoseconds) — neither is handled by the
            # explicit format strings in Silver's parser.
            ts = row['_event_time']
            try:
                # pandas Timestamp: strftime to microsecond precision, no timezone
                event_time_str = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")
            except Exception:
                event_time_str = str(ts)

            topic, event, container_no = self._build_event_from_row(
                row, idx, event_time_str
            )
            if self.publish_event(topic, event, container_no):
                published += 1
            time.sleep(inter_event_delay)

        clock.record_published(published)
        clock.advance(advance_days)
        logger.info(f"[SimWindow] Published {published} events.")
        return published

    def generate_synthetic_events(self, count: int = 100):
        """Generate synthetic container events"""
        import random
        
        containers = [f"ABCD{i:07d}" for i in range(1, 51)]
        facilities = ['DEPOT_A', 'DEPOT_B', 'YARD_1', 'YARD_2']
        event_types = list(self.TOPIC_MAPPING.keys())
        
        published_count = 0
        base_time = datetime.utcnow() - timedelta(days=7)
        
        for i in range(count):
            container = random.choice(containers)
            event_type = random.choice(event_types)
            topic = self.TOPIC_MAPPING[event_type]
            
            event_time = base_time + timedelta(hours=random.randint(0, 168))
            
            event = {
                'event_id': f"{event_type}_{i}_{int(time.time())}",
                'event_type': event_type,
                'event_time': event_time.isoformat(),
                'container_no_raw': container,
                'facility': random.choice(facilities),
                'source_file': 'synthetic'
            }
            
            # Add random fields based on type (TOPIC_MAPPING has GATE_IN/GATE_OUT, not 'gate')
            if event_type in ('GATE_IN', 'GATE_OUT'):
                event['truck'] = f"TRK{random.randint(1000, 9999)}"
            
            if self.publish_event(topic, event, container):
                published_count += 1
            
            time.sleep(0.02)
        
        logger.info(f"Generated and published {published_count} synthetic events")
        return published_count
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Container Event Producer – chronological simulation replay',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  loop (default)   Simulation mode: advance day-by-day through all data in
                   chronological order.  Event timestamps are preserved from
                   source data.  State is persisted so restarts resume from
                   where they left off.

  all              Legacy mode: publish all events per-source-file (no sorting,
                   no simulation clock).

  once             Publish a single file or event type (--file / --type).
  synthetic        Generate random synthetic events.
        """
    )
    parser.add_argument('--mode', choices=['once', 'loop', 'simulation', 'synthetic', 'all'],
                        default='loop',
                        help='Run mode (default: loop = simulation)')
    parser.add_argument('--type', choices=['gate', 'cleaning', 'mnr', 'yard_move', 'inspection'],
                        help='Specific event type to publish (once mode)')
    parser.add_argument('--file', help='Path to specific data file (once mode)')
    parser.add_argument('--interval', type=int, default=60,
                        help='Sleep interval in seconds between simulation windows (default: 60)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit records per event type (legacy all/once modes)')
    parser.add_argument('--count', type=int, default=100,
                        help='Number of synthetic events to generate')
    # Simulation-specific flags
    parser.add_argument('--sim-advance-days', type=int, default=1,
                        help='Simulation days to advance per loop iteration (default: 1)')
    parser.add_argument('--reset-sim', action='store_true',
                        help='Delete simulation state and restart from the beginning')
    parser.add_argument('--sim-data-start', type=str, default=None,
                        help='Override data start date (YYYY-MM-DD). Skips earlier data entirely.')
    parser.add_argument('--sim-data-end', type=str, default=None,
                        help='Override data end date (YYYY-MM-DD). Ignores later data.')
    parser.add_argument('--inter-event-delay', type=float, default=0.001,
                        help=(
                            'Sleep (seconds) between publishing consecutive events within one '
                            'simulation window (default: 0.001 = 1ms). '
                            'Lower values → faster throughput. '
                            'Set to 0 for maximum speed (local Kafka handles it fine). '
                            'Original default was 0.005 (5ms) = ~7 min for full dataset. '
                            '0.001 ≈ 90s, 0 ≈ 30s for 87k events.'
                        ))

    args = parser.parse_args()

    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    producer = ContainerEventProducer(bootstrap_servers)

    try:
        # ---------------------------------------------------------- once mode
        if args.mode == 'once':
            if args.file:
                logger.info(f"Publishing events from {args.file}")
                if 'gate' in args.file.lower():
                    producer.publish_gate_events(args.file, args.limit)
                elif 'clean' in args.file.lower():
                    producer.publish_cleaning_events(args.file, args.limit)
                elif 'mnr' in args.file.lower():
                    producer.publish_mnr_events(args.file, args.limit)
                elif 'yard' in args.file.lower() or 'movement' in args.file.lower():
                    producer.publish_yard_move_events(args.file, args.limit)
                elif 'inspection' in args.file.lower() or 'damage' in args.file.lower():
                    producer.publish_inspection_events(args.file, args.limit)
            elif args.type:
                filepath = producer.DATA_FILES.get(args.type)
                if not filepath:
                    logger.error(f"Unknown event type: {args.type}")
                    return
                logger.info(f"Publishing {args.type} events from {filepath}")
                if args.type == 'gate':
                    producer.publish_gate_events(filepath, args.limit)
                elif args.type == 'cleaning':
                    producer.publish_cleaning_events(filepath, args.limit)
                elif args.type == 'mnr':
                    producer.publish_mnr_events(filepath, args.limit)
                elif args.type == 'yard_move':
                    producer.publish_yard_move_events(filepath, args.limit)
                elif args.type == 'inspection':
                    producer.publish_inspection_events(filepath, args.limit)
            else:
                logger.error("Specify either --file or --type in 'once' mode")

        # ----------------------------------------------------------- all mode (legacy)
        elif args.mode == 'all':
            logger.info("Legacy mode: publishing all event types (unsorted)…")
            producer.publish_all_events(args.limit)

        # --------------------------------- loop / simulation mode (recommended)
        elif args.mode in ('loop', 'simulation'):
            inter_event_delay = args.inter_event_delay
            logger.info(
                f"[Simulation] Starting day-by-day replay  "
                f"advance={args.sim_advance_days}d  "
                f"interval={args.interval}s  "
                f"inter-event-delay={inter_event_delay}s"
            )

            # Load the unified chronological stream once (re-loaded each cycle
            # to pick up any hot-updated CSV files).
            all_events = producer.load_all_events_sorted()
            if all_events.empty:
                logger.error("No events loaded – check DATA_FILES paths.")
                return

            # Allow user to narrow the simulation window to avoid replaying
            # years of sparse/irrelevant historical data.
            if args.sim_data_start:
                cutoff = pd.Timestamp(args.sim_data_start)
                before = len(all_events)
                all_events = all_events[all_events['_event_time'] >= cutoff].reset_index(drop=True)
                logger.info(
                    f"[SimFilter] --sim-data-start={args.sim_data_start}: "
                    f"dropped {before - len(all_events)} events before cutoff, "
                    f"{len(all_events)} remain."
                )
            if args.sim_data_end:
                cutoff = pd.Timestamp(args.sim_data_end)
                before = len(all_events)
                all_events = all_events[all_events['_event_time'] <= cutoff].reset_index(drop=True)
                logger.info(
                    f"[SimFilter] --sim-data-end={args.sim_data_end}: "
                    f"dropped {before - len(all_events)} events after cutoff, "
                    f"{len(all_events)} remain."
                )
            if all_events.empty:
                logger.error("No events remain after date filtering – check --sim-data-start/end.")
                return

            data_start = all_events['_event_time'].min().to_pydatetime()
            data_end   = all_events['_event_time'].max().to_pydatetime()

            # Handle --reset-sim before initialising the clock
            state_path = SimulationClock.STATE_FILE
            if args.reset_sim and os.path.exists(state_path):
                os.remove(state_path)
                logger.info("[SimClock] State reset per --reset-sim flag.")

            clock = SimulationClock(
                data_start=data_start,
                data_end=data_end,
            )

            logger.info(
                f"[SimClock] data_start={data_start.date()}  "
                f"data_end={data_end.date()}  "
                f"current_sim_date={clock.sim_date.date()}"
            )

            while True:
                # Reload on each cycle so CSV hot-updates are picked up
                all_events = producer.load_all_events_sorted()

                published = producer.publish_simulation_window(
                    clock=clock,
                    all_events=all_events,
                    advance_days=args.sim_advance_days,
                    inter_event_delay=inter_event_delay,
                )

                if published > 0:
                    # Only sleep when there were real events – gives Spark
                    # time to process the batch before next window arrives.
                    logger.info(
                        f"[Loop] Next sim_date={clock.sim_date.date()}  "
                        f"sleeping {args.interval}s…"
                    )
                    time.sleep(args.interval)
                else:
                    # Empty window → fast-forward immediately (no sleep)
                    logger.debug(
                        f"[Loop] Empty window, fast-forwarding to {clock.sim_date.date()}"
                    )

        # ------------------------------------------------------- synthetic mode
        elif args.mode == 'synthetic':
            logger.info(f"Generating {args.count} synthetic events")
            producer.generate_synthetic_events(args.count)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")

    finally:
        producer.close()


if __name__ == '__main__':
    main()
