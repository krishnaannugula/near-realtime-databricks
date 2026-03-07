import json
import uuid
import random
import time
import os
import sys
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData


class TelecomEventGenerator:
    """Generate realistic CDR and EDR events from different source systems"""
    
    def __init__(self, cdr_conn_str, edr_conn_str, cdr_hub, edr_hub):
        self.cdr_producer = EventHubProducerClient.from_connection_string(
            cdr_conn_str, eventhub_name=cdr_hub
        )
        self.edr_producer = EventHubProducerClient.from_connection_string(
            edr_conn_str, eventhub_name=edr_hub
        )
        
        # Tariff plans
        self.tariff_plans = {
            'voice': {'local': 0.05, 'roaming': 0.45},
            'data': {'local': 0.10, 'roaming': 1.50},
            'sms': {'local': 0.02, 'roaming': 0.15}
        }
        
        # Network elements
        self.network_elements = ['MSC-01', 'MSC-02', 'SGSN-01', 'SGSN-02', 'GGSN-01']
        self.rating_engines = ['RE-PRIMARY', 'RE-BACKUP']
        # MSISDN reuse settings: keep a pool and prefer MSISDNs used
        # within the last `reuse_window_days` days. Pool can be
        # seeded from `msisdn_list.txt` or persisted in `msisdn_pool.json`.
        self.pool_size = int(os.environ.get('MSISDN_POOL_SIZE', 5))
        self.reuse_rate = float(os.environ.get('MSISDN_REUSE_RATE', 0.6))
        self.reuse_window_days = int(os.environ.get('MSISDN_REUSE_DAYS', 3))

        self.pool_file = os.path.join(os.path.dirname(__file__), 'msisdn_pool.json')
        self.msisdn_pool = []  # list of {'msisdn': str, 'last_used': iso}

        # Load user-provided simple list first (one MSISDN per line)
        user_list = os.path.join(os.path.dirname(__file__), 'msisdn_list.txt')
        if os.path.exists(user_list):
            try:
                with open(user_list, 'r', encoding='utf-8') as f:
                    for line in f:
                        v = line.strip()
                        if v:
                            self.msisdn_pool.append({'msisdn': v, 'last_used': datetime.utcnow().isoformat()})
            except Exception:
                pass

        # Load persisted pool if available
        if os.path.exists(self.pool_file):
            try:
                with open(self.pool_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'msisdn' in item and 'last_used' in item:
                            self.msisdn_pool.append(item)
            except Exception:
                pass

        # Seed pool up to pool_size
        while len(self.msisdn_pool) < self.pool_size:
            self.msisdn_pool.append({'msisdn': f"+358{random.randint(10000000, 99999999)}", 'last_used': datetime.utcnow().isoformat()})
    
    def generate_msisdn(self):
        """Generate phone number"""
        # With probability `reuse_rate` return an existing MSISDN
        if self.msisdn_pool and random.random() < self.reuse_rate:
            # Extract msisdn string from dict and update timestamp
            item = random.choice(self.msisdn_pool)
            item['last_used'] = datetime.utcnow().isoformat()
            return item['msisdn']  # Return string, not dict

        # Otherwise generate a new MSISDN and add/rotate into the pool
        new_msisdn = f"+358{random.randint(10000000, 99999999)}"
        new_item = {'msisdn': new_msisdn, 'last_used': datetime.utcnow().isoformat()}
        
        if len(self.msisdn_pool) < self.pool_size:
            self.msisdn_pool.append(new_item)
        else:
            # Occasionally rotate out an entry so pool evolves
            if random.random() < 0.2:
                idx = random.randrange(self.pool_size)
                self.msisdn_pool[idx] = new_item

        return new_msisdn  # Return string
    
    def generate_cdr(self, scenario='good'):
        """
        Generate CDR from Network Elements (MSC, SGSN, GGSN)
        CDRs have their own call_id from network
        """
        msisdn = self.generate_msisdn()
        call_id = f"CALL-{uuid.uuid4().hex[:12].upper()}"  # Unique network call ID
        
        service = random.choice(['voice', 'data', 'sms'])
        is_roaming = random.random() < 0.20  # 20% roaming
        
        # Determine scenario
        if scenario == 'good':
            if service == 'voice':
                duration = random.randint(30, 3600)  # seconds
                volume = round(duration / 60, 2)  # convert to minutes
                unit = 'minutes'
            elif service == 'data':
                volume = round(random.uniform(1, 500), 2)  # MB
                unit = 'MB'
                duration = None
            else:  # sms
                volume = random.randint(1, 50)
                unit = 'count'
                duration = None
            
            status = 'COMPLETED'
            data_quality = 'GOOD'
        
        elif scenario == 'bad_incomplete':
            # Missing some fields
            volume = random.randint(1, 100) if service != 'voice' else round(random.uniform(1, 60), 2)
            unit = 'minutes' if service == 'voice' else ('MB' if service == 'data' else 'count')
            duration = random.randint(30, 600) if service == 'voice' else None
            status = 'COMPLETED'
            data_quality = 'BAD'
        
        else:  # scenario == 'incomplete'
            # Missing critical fields
            volume = None
            unit = None
            duration = None
            status = 'PARTIAL'
            data_quality = 'BAD'
        
        # Tariff calculation
        rate = self.tariff_plans[service]['roaming' if is_roaming else 'local']
        expected_charge = round((volume or 0) * rate, 4) if volume else None
        
        cdr = {
            'record_type': 'CDR',
            'source_system': 'Network Elements',
            'call_id': call_id,  # Network call ID - different from EDR session_id
            'msisdn': msisdn,
            'imsi': f"358{random.randint(10, 99)}{random.randint(100000000, 999999999)}",
            'timestamp': datetime.utcnow().isoformat(),
            'service_type': service,
            'volume': volume,
            'unit': unit,
            'duration_seconds': duration,
            'is_roaming': is_roaming,
            'roaming_country': 'US' if is_roaming else None,
            'network_element': random.choice(self.network_elements),
            'cell_id': f"CELL-{random.randint(1000, 9999)}",
            'status': status,
            'expected_charge': expected_charge,
            'data_quality': data_quality,
            'sequence_number': random.randint(100000, 999999),
            'incomplete': volume is None if scenario == 'incomplete' else False
        }
        
        return cdr
    
    def generate_edr(self, cdr, scenario='good'):
        """
        Generate EDR from Online Charging System (OCS)
        EDRs have their own session_id (different from CDR call_id)
        """
        session_id = f"SESSION-{uuid.uuid4().hex[:12].upper()}"  # Unique OCS session ID
        
        # EDR processing delay (usually 30 seconds to 5 minutes after CDR)
        edr_timestamp = datetime.fromisoformat(cdr['timestamp']) + timedelta(
            seconds=random.randint(30, 300)
        )
        
        if scenario == 'good':
            charge = cdr['expected_charge'] if cdr['expected_charge'] else 0.0
            billing_status = 'SUCCESS'
            data_quality = 'GOOD'
        
        elif scenario == 'overcharge':
            # Overcharge by 10-50%
            base_charge = cdr['expected_charge'] or 0.0
            charge = round(base_charge * random.uniform(1.10, 1.50), 4)
            billing_status = 'SUCCESS'
            data_quality = 'BAD'
        
        elif scenario == 'zero_rated':
            # Should charge but doesn't
            charge = 0.0
            billing_status = 'SUCCESS'
            data_quality = 'BAD'
        
        elif scenario == 'duplicate':
            # Duplicate charge
            charge = cdr['expected_charge'] if cdr['expected_charge'] else 0.0
            billing_status = 'SUCCESS'
            data_quality = 'BAD'
        
        elif scenario == 'failure':
            # Billing failed
            charge = 0.0
            billing_status = 'FAILED'
            data_quality = 'BAD'
        
        else:  # phantom
            # EDR without CDR
            charge = round(random.uniform(1, 50), 2)
            billing_status = 'SUCCESS'
            data_quality = 'BAD'
        
        edr = {
            'record_type': 'EDR',
            'source_system': 'Online Charging System',
            'session_id': session_id,  # Unique OCS session ID (not same as CDR call_id)
            'msisdn': cdr['msisdn'],
            'cdr_call_id': cdr['call_id'],  # Reference to CDR
            'timestamp': edr_timestamp.isoformat(),
            'charge_amount': charge,
            'currency': 'EUR',
            'account_balance_before': round(random.uniform(5, 200), 2),
            'account_balance_after': round(random.uniform(0, 195), 2),
            'payment_method': random.choice(['PREPAID_DEDUCT', 'POSTPAID_INVOICE']),
            'rating_engine': random.choice(self.rating_engines),
            'rating_group': f"RG-{random.randint(100, 999)}",
            'billing_status': billing_status,
            'transaction_id': str(uuid.uuid4()),
            'data_quality': data_quality,
            'processing_latency_ms': round(random.uniform(100, 5000), 2)
        }
        
        return edr
    
    def send_event(self, producer, event_data):
        """Send event to Event Hub"""
        try:
            producer.send_batch(
                [EventData(json.dumps(event_data))],
                partition_key=event_data.get('msisdn')
            )
            quality = event_data.get('data_quality', 'GOOD')
            record_type = event_data.get('record_type')
            print(f"✓ Sent {record_type} [{quality}] - {event_data['msisdn']}")
        except Exception as e:
            print(f"✗ Error sending event: {e}")
    
    def generate_and_send(self):
        """
        Generate and send CDR and EDR events with realistic scenarios
        
        Distribution of scenarios:
        - Good records: 85% (for successful reconciliation)
        - Bad records: 15% (various data quality issues)
        """
        # Scenario distribution
        rand = random.random()
        
        if rand < 0.85:
            cdr_scenario = 'good'
            edr_scenario = 'good'
        elif rand < 0.88:
            cdr_scenario = 'good'
            edr_scenario = 'overcharge'
        elif rand < 0.90:
            cdr_scenario = 'good'
            edr_scenario = 'zero_rated'
        elif rand < 0.92:
            cdr_scenario = 'bad_incomplete'
            edr_scenario = 'good'
        elif rand < 0.94:
            cdr_scenario = 'good'
            edr_scenario = 'duplicate'
        elif rand < 0.96:
            cdr_scenario = 'good'
            edr_scenario = 'failure'
        else:
            cdr_scenario = 'incomplete'
            edr_scenario = 'phantom'
        
        # Generate and send CDR
        cdr = self.generate_cdr(cdr_scenario)
        self.send_event(self.cdr_producer, cdr)
        
        # Generate and send EDR (only if CDR is complete)
        if not cdr.get('incomplete', False):
            edr = self.generate_edr(cdr, edr_scenario)
            self.send_event(self.edr_producer, edr)
        else:
            print(f"⚠ Skipped EDR for incomplete CDR: {cdr['call_id']}")
    
    def close(self):
        """Close Event Hub connections"""
        self.cdr_producer.close()
        self.edr_producer.close()


def main():
    """Main loop - generate realistic events every 10 seconds"""
    
    # Event Hub Configuration
    # Get these from Azure Portal > Event Hubs > Your Namespace > Shared access policies > RootManageSharedAccessKey
    CDR_CONNECTION_STRING = "<<EventsHub connection string>>"
    EDR_CONNECTION_STRING = "<<EventsHub connection string>"
    
    # Event Hub names - must match what you created in Azure
    CDR_HUB = "cdr_hub"
    EDR_HUB = "edr_hub"
    
    # Initialize generator
    try:
        generator = TelecomEventGenerator(
            CDR_CONNECTION_STRING,
            EDR_CONNECTION_STRING,
            CDR_HUB,
            EDR_HUB
        )
    except Exception as e:
        print(f"✗ Failed to connect to Event Hubs: {e}")
        print("Check your connection strings and Event Hub names in Azure Portal")
        return
    
    print("=" * 70)
    print("TELECOM CDR/EDR EVENT GENERATOR")
    print("=" * 70)
    print(f"CDR Source: Network Elements (MSC, SGSN, GGSN)")
    print(f"EDR Source: Online Charging System (OCS)")
    print(f"CDR Event Hub: {CDR_HUB}")
    print(f"EDR Event Hub: {EDR_HUB}")
    print(f"Namespace: revenuemanagement.servicebus.windows.net")
    print("-" * 70)
    print("Data Distribution:")
    print("  - Good Records: 85% (for successful revenue reconciliation)")
    print("  - Overcharge: 3% (billing higher than usage)")
    print("  - Zero-rated: 2% (should charge but doesn't)")
    print("  - Incomplete CDR: 2% (missing fields)")
    print("  - Duplicate Billing: 2% (multiple charges)")
    print("  - Billing Failure: 2% (failed transactions)")
    print("  - Phantom Charges: 2% (EDR without CDR)")
    print("-" * 70)
    print("Generating events every 10 seconds... Press Ctrl+C to stop")
    print("=" * 70)
    print()
    
    try:
        event_count = 0
        while True:
            event_count += 1
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Generating event set #{event_count}...")
            generator.generate_and_send()
            print()
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("\nStopping event generator...")
        generator.close()
        print(f"Total event sets generated: {event_count}")
        print("Done.")


if __name__ == "__main__":
    main()
