import pandas as pd

patients = pd.read_csv('data/synthea/patients.csv')
encounters = pd.read_csv('data/synthea/encounters.csv')
procedures = pd.read_csv('data/synthea/procedures.csv')
cms_data = pd.read_csv('data/cms/FY_2025_Hospital_Readmissions_Reduction_Program_Hospital.csv')


patients = patients.rename(columns={'Id': 'patient_id'})
encounters = encounters.rename(columns={'PATIENT': 'patient_id'})
procedures = procedures.rename(columns={'PATIENT': 'patient_id'})

patient_encounters = pd.merge(patients, encounters, on='patient_id')
patient_encounters.fillna(0, inplace=True)

patient_procedures = pd.merge(patient_encounters, procedures, on='patient_id')
patient_procedures.fillna(0, inplace=True)

provider_productivity = encounters.groupby('PROVIDER').size().reset_index(name='encounter_count')

patient_encounters['START'] = pd.to_datetime(patient_encounters['START'])
patient_encounters = patient_encounters.sort_values(by=['patient_id', 'START'])
patient_encounters['prev_encounter_date'] = patient_encounters.groupby('patient_id')['START'].shift(1)
patient_encounters['days_between_appointments'] = (patient_encounters['START'] - patient_encounters['prev_encounter_date']).dt.days
appointment_analytics = patient_encounters.groupby('patient_id')['days_between_appointments'].mean().reset_index(name='avg_days_between_appointments')

appointment_analytics.fillna(0, inplace=True)

cms_data.fillna(0, inplace=True)




appointment_analytics = patient_encounters.groupby('patient_id')['days_between_appointments'].mean().reset_index(name='avg_days_between_appointments')

readmission_rates = cms_data[['Facility ID', 'Excess Readmission Ratio', 'Number of Readmissions', 'Number of Discharges']]

readmission_rates['Number of Readmissions'] = pd.to_numeric(readmission_rates['Number of Readmissions'], errors='coerce').astype(float)
readmission_rates['Number of Discharges'] = pd.to_numeric(readmission_rates['Number of Discharges'], errors='coerce').astype(float)

readmission_rates['Readmission Rate'] = (readmission_rates['Number of Readmissions'] / readmission_rates['Number of Discharges']) * 100

provider_productivity.to_csv('data/transformed/provider_productivity.csv', index=False)
appointment_analytics.to_csv('data/transformed/appointment_analytics.csv', index=False)
cms_data.to_csv('data/transformed/cms_data.csv', index=False)
readmission_rates.to_csv('data/transformed/readmission_rates.csv', index=False)
print("Columns in provider_productivity table:", provider_productivity.columns.tolist())