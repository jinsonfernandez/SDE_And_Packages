# Replacing single vlaue using dict
names['GENDER'].replace({'MALE': 'BOY' , 'FEMALE': 'GIRL'})

#replacing multiple values using dict

names.replace({'Ethnicity': {'ASIAN AND PACI': 'ASIAN'
,
'ASIAN AND PACIFIC ISLANDER': 'ASIAN'
,
'BLACK NON HISPANIC': 'BLACK'
,
'BLACK NON HISP': 'BLACK'
,
'WHITE NON HISPANIC': 'WHITE'
,
'WHITE NON HISP': 'WHITE'}})