"""
Container Lifecycle Event Producer
Reads real staging data and publishes to Kafka topics
Supports: gate events, cleaning, M&R, yard movements, and inspections
"""
import json
import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List
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

    
    # Map event types to Kafka topics
    TOPIC_MAPPING = {
        # Gate
        'GATE_IN': 'raw.gate',
        'GATEIN': 'raw.gate',
        'GATE_OUT': 'raw.gate',
        'GATEOUT': 'raw.gate',

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
        'CLEANED': 'raw.cleaning',

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
            event['ingest_time'] = datetime.utcnow().isoformat()
            
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
            event_type = str(row.get('event_type', '')).strip()
            if not event_type or event_type not in self.TOPIC_MAPPING:
                continue
            
            topic = self.TOPIC_MAPPING[event_type]
            container_no = str(row.get('container_no_raw', ''))
            
            event = {
                'event_id': f"GATE_{idx}_{int(time.time())}",
                'event_type': event_type,
                'event_time': str(row.get('event_time', '')),
                'date_raw': str(row.get('date_raw', '')),
                'time_raw': str(row.get('time_raw', '')),
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
                'position': str(row.get('position', '')),
                'location': str(row.get('location', '')),
                'remark': str(row.get('remark', '')),
                'nominate_remark': str(row.get('nominate_remark', '')),
                'facility': self._extract_ct_facility(str(row.get('location', ''))),
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
                'event_id': f"CLEAN_{idx}_{int(time.time())}",
                'event_type': event_type,
                'event_time': str(row.get('event_time', '')),
                'date_in': str(row.get('Date In', row.get('Date_In', ''))),
                'container_no_raw': container_no,
                'type_raw': str(row.get('type_raw', '')),
                'remark_raw': str(row.get('remark_raw', '')),
                'amount': str(row.get('amount', '')),
                'facility': self._extract_ct_facility(str(row.get('facility', ''))),
                'source_file': str(row.get('source_file', os.path.basename(filepath))),
                'source_sheet': str(row.get('source_sheet', '')),
                'source_row': str(row.get('source_row', idx)),
                'is_synthetic': str(row.get('is_synthetic', '0'))
            }
            # If event_time missing, fallback to date_in
            if not str(event.get('event_time', '')).strip():
                event['event_time'] = str(event.get('date_in', '')).strip()

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
            event_type = str(row.get('event_type', 'MNR_RECEIVED')).strip()
            if event_type not in self.TOPIC_MAPPING:
                event_type = 'MNR_RECEIVED'
            
            topic = self.TOPIC_MAPPING.get(event_type, 'raw.mnr')
            container_no = str(row.get('container_no_raw', ''))
            
            event = {
                'event_id': f"MNR_{idx}_{int(time.time())}",
                'event_type': event_type,
                'event_time': str(row.get('event_time', '')),
                'container_no_raw': container_no,
                'size_raw': str(row.get('size_raw', '')),
                'location_raw': str(row.get('location_raw', '')),
                'amount_raw': str(row.get('amount_raw', '')),
                'cleaning_cost_raw': str(row.get('cleaning_cost_raw', '')),
                'repair_cost_raw': str(row.get('repair_cost_raw', '')),
                'discount_raw': str(row.get('discount_raw', '')),
                'note_raw': str(row.get('note_raw', '')),
                'stage': str(row.get('stage', '')),
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
            
            from_location = f"{row.get('from_block', '')}-{row.get('from_row', '')}-{row.get('from_bay', '')}-{row.get('from_tier', '')}"
            to_location = f"{row.get('to_block', '')}-{row.get('to_row', '')}-{row.get('to_bay', '')}-{row.get('to_tier', '')}"
            
            event = {
                'event_id': str(row.get('move_id', f"YM_{idx}_{int(time.time())}")),
                'event_type': 'yard_move',
                'event_time': str(row.get('move_time', '')),
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
                'source_file': os.path.basename(filepath)
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
            
            event = {
                'event_id': str(row.get('inspection_id', f"INSP_{idx}_{int(time.time())}")),
                'event_type': 'inspection',
                'event_time': str(row.get('inspection_time', '')),
                'container_no_raw': container_no,
                'facility': str(row.get('facility', 'UNKNOWN')),
                'damage_code': str(row.get('damage_code', '')),
                'component': str(row.get('component', '')),
                'severity': str(row.get('severity', '')),
                'estimated_cost': str(row.get('estimated_cost', '')),
                'currency': str(row.get('currency', '')),
                'inspector_id': str(row.get('inspector_id', '')),
                'photo_ref': str(row.get('photo_ref', '')),
                'remarks': str(row.get('remarks', '')),
                'source': str(row.get('source', os.path.basename(filepath)))
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
            
            # Add random fields based on type
            if event_type == 'gate':
                event['gate_type'] = random.choice(['IN', 'OUT'])
                event['truck_no'] = f"TRK{random.randint(1000, 9999)}"
                event['gross_weight'] = random.randint(5000, 30000)
            
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
    parser = argparse.ArgumentParser(description='Container Event Producer for Real Data')
    parser.add_argument('--mode', choices=['once', 'loop', 'synthetic', 'all'], 
                       default='all', help='Run mode (all=publish all event types)')
    parser.add_argument('--type', choices=['gate', 'cleaning', 'mnr', 'yard_move', 'inspection'],
                       help='Specific event type to publish')
    parser.add_argument('--file', help='Path to specific data file (overrides --type)')
    parser.add_argument('--interval', type=int, default=60,
                       help='Interval in seconds for loop mode')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of records per event type')
    parser.add_argument('--count', type=int, default=100,
                       help='Number of synthetic events to generate')
    
    args = parser.parse_args()
    
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    producer = ContainerEventProducer(bootstrap_servers)
    
    try:
        if args.mode == 'once':
            if args.file:
                logger.info(f"Publishing events from {args.file}")
                df = producer.load_csv_data(args.file)
                # Auto-detect file type and use appropriate publisher
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
                logger.error("Must specify either --file or --type in 'once' mode")
        
        elif args.mode == 'all':
            logger.info("Publishing all event types...")
            producer.publish_all_events(args.limit)
        
        elif args.mode == 'loop':
            logger.info(f"Running in loop mode with {args.interval}s interval")
            while True:
                producer.publish_all_events(args.limit)
                logger.info(f"Waiting {args.interval} seconds...")
                time.sleep(args.interval)
        
        elif args.mode == 'synthetic':
            logger.info(f"Generating {args.count} synthetic events")
            producer.generate_synthetic_events(args.count)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    finally:
        producer.close()


if __name__ == '__main__':
    main()
