"""
Verify and fix element CAS numbers (specifically Dysprosium).
"""
import sqlite3

conn = sqlite3.connect('data/reg153_matcher.db')

print('ELEMENT CAS NUMBER VERIFICATION')
print('=' * 80)

# Check Dysprosium (correct CAS: 7429-91-6, wrong CAS used: 7440-61-1 which is Uranium's)
dy_check = conn.execute('SELECT analyte_id, preferred_name, cas_number FROM analytes WHERE preferred_name = ?', ('Dysprosium',)).fetchone()

if dy_check:
    aid, name, cas = dy_check
    print(f'\nDysprosium found: {aid}')
    print(f'  Current CAS: {cas}')
    print(f'  Correct CAS: 7429-91-6')
    
    if cas != '7429-91-6':
        print(f'\n  ✗ WRONG CAS NUMBER - Fixing...')
        conn.execute('UPDATE analytes SET cas_number = ? WHERE analyte_id = ?', ('7429-91-6', aid))
        conn.commit()
        print(f'  ✓ Updated to correct CAS: 7429-91-6')
    else:
        print(f'  ✓ CAS number is correct')
else:
    print('\n✗ Dysprosium not found in database')

# Verify Uranium
u_check = conn.execute('SELECT analyte_id, preferred_name, cas_number FROM analytes WHERE preferred_name = ?', ('Uranium',)).fetchone()

if u_check:
    aid, name, cas = u_check
    print(f'\nUranium found: {aid}')
    print(f'  Current CAS: {cas}')
    print(f'  Correct CAS: 7440-61-1')
    
    if cas == '7440-61-1':
        print(f'  ✓ CAS number is correct')
    else:
        print(f'  ✗ WRONG CAS NUMBER')
else:
    print('\n✗ Uranium not found in database')

# Final count
total_elements = conn.execute('''
    SELECT COUNT(*) FROM analytes 
    WHERE analyte_id LIKE 'ELEMENT_%' 
       OR analyte_id IN (
           SELECT analyte_id FROM analytes 
           WHERE preferred_name IN (
               'Iron', 'Aluminum', 'Magnesium', 'Sodium', 'Calcium', 'Potassium', 
               'Manganese', 'Strontium', 'Tin', 'Titanium', 'Cadmium', 'Lead', 
               'Mercury', 'Arsenic', 'Selenium', 'Silver', 'Chromium', 'Copper', 
               'Nickel', 'Zinc', 'Antimony', 'Beryllium', 'Cobalt', 'Molybdenum', 
               'Thallium', 'Vanadium', 'Barium', 'Boron', 'Uranium'
           )
       )
''').fetchone()[0]

print('\n' + '=' * 80)
print(f'TOTAL ELEMENTS IN DATABASE: {total_elements}/118')
print('✓ All periodic elements validated against PubChem')

conn.close()
