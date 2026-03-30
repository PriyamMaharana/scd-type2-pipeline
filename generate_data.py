import pandas as pd
from faker import Faker
from datetime import date, timedelta
import random

fake = Faker('en_IN')
random.seed(42)

CITIES = ['Mumbai', 'Delhi', 'Bengaluru', 'Chennai', 'Pune', 'Hyderabad',
          'Kolkata', 'Jamshedpur', 'Bhubaneswar', 'Ahmedabad', 'Mysuru']

STATES = {
    'Mumbai': 'Maharashtra', 'Delhi': 'Delhi', 'Bengaluru': 'Karnataka',
    'Chennai': 'Tamil Nadu', 'Pune': 'Maharashtra', 'Hyderabad': 'Telangana',
    'Kolkata': 'West Bengal', 'Jamshedpur': 'Jharkhand', 'Bhubaneswar': 'Odisha',
    'Ahmedabad': 'Gujarat', 'Mysuru': 'Karnataka'
}

SEGMENTS = ['Regular', 'Premium', 'VIP']


def generate_initial_load(n=1000):
    ''' First time we load 1000 customers '''
    records = []
    for i in range(1, n+1):
        city = random.choice(CITIES)
        records.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': f'customer{i}@gmail.com',
            'city': city,
            'state': STATES[city],
            'customer_segment': random.choice(SEGMENTS),
            'source_date': date(2024, 1, 1)
        })
    df = pd.DataFrame(records)
    df.to_csv('data/initial_load.csv', index=False)
    print(f'Generated initial_load.csv - {len(df)} records.')
    return df


def generate_delta_load(base_df, batch_num, change_date, 
                        pct_changed=0.15, pct_new=0.05):
    '''
    Simulates an incremental load
    - pct_changed: % of existing customers who changed something
    - pct_new: % brand new customers added
    '''
    records = []
    
    #1. Customer who changed city or segment
    changed_id = random.sample(
        list(base_df['customer_id']),
        int(len(base_df)*pct_changed)
    )
    
    for _, row in base_df[
            base_df['customer_id'].isin(changed_id)].iterrows():
        r = row.to_dict()
        
        #Randomly changes either city or segment or both
        change_type = random.choice(['city','segment','both'])
        
        if change_type in ['city', 'both']:
            new_city = random.choice(
                [c for c in CITIES if c != r['city']]
            )
            r['city'] = new_city
            r['state'] = STATES[new_city]
            
        if change_type in ['segment', 'both']:
            r['customer_segment'] = random.choice(
                [s for s in SEGMENTS if s != r['customer_segment']]
            )
        
        r['source_date'] = change_date
        records.append(r)
        
        #2. Brand new customer
        max_id = base_df['customer_id'].max()
        new_count = int(len(base_df) * pct_new)
        
        for i in range(1, new_count + 1):
            city = random.choice(CITIES)
            records.append({
                'customer_id': max_id + i,
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': f"customer{max_id + i}@example.com",
                'city': city,
                'state': STATES[city],
                'customer_segment': random.choice(SEGMENTS),
                'source_date': change_date
            })
        
        df = pd.DataFrame(records)
        df.to_csv(f'data/delta_load_{batch_num}.csv', index=False)
        print(f"Generated delta_load_{batch_num}.csv — "
            f"{len(changed_id)} changed, {new_count} new")
        return df


if __name__ == "__main__":
    import os
    os.makedirs('data', exist_ok=True)
    
    initial = generate_initial_load(1000)
    generate_delta_load(initial, 1, date(2024, 2, 1))
    generate_delta_load(initial, 2, date(2024, 3, 1))
    generate_delta_load(initial, 3, date(2024, 4, 1))
    
    print(f'\nAll date files generated in /data folder.')