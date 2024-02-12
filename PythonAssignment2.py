import pandas as pd
# Load the data to a single DataFrame.
apt_evaluation  = pd.read_csv('/tmp/ML-UToronto/Python/assignment_2/Apartment Building Evaluations 2023 - current.csv')

# 2 Profile the DataFrame.
#     What are the column names?
#     What are the dtypes when loaded? Do any not make sense?
#     How many NaNs are in each column?
#     What is the shape of the DataFrame?

# IT is informating the amount of rows and columns
print(f'Number of Rows is {len(apt_evaluation)} and Number of Columns is {len(apt_evaluation.columns)}' )

# Loop on all columns and check type and amount of NaNs in each column
for col in apt_evaluation.columns:
    print(f'Column {col} with data type { apt_evaluation[col].dtype}  has {apt_evaluation[col].isna().sum()} NaN')


# Generate some summary statistics for the data.
#     For numeric columns: What are the max, min, mean, and median?
#     Are there any statistics that seem unexpected?
#Function is returning max, min, mean, and median on each column
def minMaxMeanMedian(x):
    return pd.Series(index=['Min','Max','Mean','Median'],data=[x.min(),x.max(),x.mean(),x.median()]).map('{:,.0f}'.format)

# It is calling the minMaxMeanMedian select only numeric columns
apt_evaluation.select_dtypes(include='number').apply(minMaxMeanMedian)


#     For text columns: What is the most common value? How many unique values are there?

#Function is returning Most common Value and Amount unique values on each column
def commonValueUnique(x):
    return pd.Series(index=['Most common Value','Amount unique values'],data=[x.value_counts().head(1).keys()[0],x.value_counts().count()])

# It is calling the commonValueUnique select only object columns
apt_evaluation.select_dtypes(include='object').apply(commonValueUnique)




# 4 Rename one or more columns in the DataFrame.
#Rename _id column name to Unique Key
apt_evaluation = apt_evaluation.rename(columns={'_id':'Unique Key'})
# Rename all columns from upper case to capitalize
apt_evaluation = apt_evaluation.rename(columns=str.capitalize)
apt_evaluation.head(5)



# 5 Select a single column and find its unique values.
print(f'Column "{apt_evaluation['Unique key'].name}" is unique = {apt_evaluation['Unique key'].is_unique}')
print(f'Column "{apt_evaluation['Site address'].name}" is unique = {apt_evaluation['Site address'].is_unique}')
print(f'Column "{apt_evaluation['Year built'].name}" is unique = {apt_evaluation['Year built'].is_unique}')

# 6 Select a single text/categorical column and find the counts of its values.
print(apt_evaluation['Wardname'].value_counts())


# 7 Convert the data type of at least one of the columns. If all columns are typed correctly, convert one to str and back.
#Using convert_dtypes function to convert columns to the best possible dtypes using dtypes supporting
apt_evaluation = apt_evaluation.convert_dtypes()
#Show all columns and the new data types
for col in apt_evaluation.columns:
    print(f'Column "{col}" has { apt_evaluation[col].dtype} data type')



# 8 Write the DataFrame to a different file format than the original
#Saving dataframe to excel format not write the index column
apt_evaluation.to_excel('/tmp/ML-UToronto/Python/assignment_2/Apartment Building Evaluations 2023 - current.xlsx',index=False)



# 1 More data wrangling, filtering
    # Create a column derived from an existing one. Some possibilities:
    # Bin a continuous variable
    # Extract a date or time part (e.g. hour, month, day of week)
    # Assign a value based on the value in another column (e.g. TTC line number based on line values in the subway delay data)
    # Replace text in a column (e.g. replacing occurrences of "Street" with "St.")

    # Assign a value based on the value in another column (e.g. TTC line number based on line values in the subway delay data)
#Adding a new column called "Street type abbr" from part of Site address information
apt_evaluation['Street type abbr'] = apt_evaluation['Site address'].str.split(" ").str[-1]
apt_evaluation[['Street type abbr','Site address']]


# Bin a continuous variable
# Label for the new column
cut_labels = ['Bad', 'Not too bad',  'Good', 'Great']
# Range defined to cut "Current building eval score" column
cut_bins =  [0,60,70,85,100]
# Add a new column called "Building classification" defiend by the cut bin range.
apt_evaluation['Building classification']= pd.cut(apt_evaluation['Current building eval score'],bins=cut_bins,labels=cut_labels)
apt_evaluation.head(5)



# Reading "Street Type.csv" file where there are a list of Street Type and abbreviation with "Street type" and "Abbreviation" columns
street_type = pd.read_csv('/tmp/ML-UToronto/Python/assignment_2/SENRA_ABRAHAO_ABRAHIM_python_assignment2_Street Type.csv')
#Adding a new column called "Street type abbr" from part of Site address information
# Adding a new column called "Street type" from "Street Type.csv" by join with "Street type abbr" where the column data matches
apt_evaluation = pd.merge(apt_evaluation,
                            street_type,
                            how='left',
                            left_on='Street type abbr',
                            right_on='Abbreviation').drop(columns=['Abbreviation'])



    # Extract a date or time part (e.g. hour, month, day of week)
# Extract Month from "Evaluation completed on" and create the "Nonth evaluated"
apt_evaluation['Nonth evaluated'] = apt_evaluation['Evaluation completed on'].astype('datetime64[ns]').dt.strftime("%B")
# Extract Day from "Evaluation completed on" and create the "Day evaluated"
apt_evaluation['Day evaluated'] = apt_evaluation['Evaluation completed on'].astype('datetime64[ns]').dt.day

apt_evaluation.head(5)



    # Replace text in a column (e.g. replacing occurrences of "Street" with "St.")
#apt_evaluation['Site address'].replace('ST','Street', regex=True)
# apt_evaluation['Site address'].replace(street_type['Abbreviation'].to_list(),street_type['Street Type'].to_list(),regex=True)
#Replace Street Type Abbreviation from "Site address" column to full name of Street Type on "street_type" list
apt_evaluation['Site address'] = apt_evaluation['Site address'].replace(street_type['Abbreviation'].to_list(),street_type['Street Type'].to_list(),regex=True)

apt_evaluation


# 2 Remove one or more columns from the dataset.
apt_evaluation = apt_evaluation.drop(columns='Tenant service request log')
print(apt_evaluation.head(3))


# 3 Extract a subset of columns and rows to a new DataFrame
    # with the .query() method and column selecting [[colnames]]
# Quering where 'Evaluation completed on' >= 6 months, select some columns
dt = (pd.Timestamp.now() + pd.DateOffset(months=-6)).strftime("%Y-%m-%d")
apt_evaluation.query("`Evaluation completed on` >= @dt")[['Unique key','Year built','Site address','Confirmed storeys','Confirmed units','Building classification']]

    # with .loc[]
great_apt_evaluation = apt_evaluation.copy()
great_apt_evaluation = great_apt_evaluation[['Unique key','Year built','Site address','Confirmed storeys','Confirmed units','Building classification']].loc[great_apt_evaluation['Building classification'] == 'Great']


# 4 Investigate null values
    # Create and describe a DataFrame containing records with NaNs in any column
    # Create and describe a DataFrame containing records with NaNs in a subset of columns
    # If it makes sense to drop records with NaNs in certain columns from the original DataFrame, do so.

# Subset of apartment where there is no Year registered information
apt_evaluation.loc[apt_evaluation['Year registered'].isna(), #filter rows
['Unique key','Year built','Site address','Confirmed storeys','Confirmed units','Building classification']] #get columns


# Grouping and aggregating
# 1 Use groupby() to split your data into groups based on one of the columns.
# 2 Use agg() to apply multiple functions on different columns and create a summary table. Calculating group sums or standardizing data are two examples of possible functions that you can use.

#Grouping data by Building classification
apt_group_by_classification = apt_evaluation.groupby('Building classification',observed=True)
apt_group_by_classification.head()

#Grouping data by Building classification and Wardname and count apartment and gettind the  moster older year built, and the most recent year built
classification_summary = apt_evaluation.groupby(['Building classification','Wardname'],observed=True).agg(apt_count=('Unique key','count'),min_year_built=('Year built','min'),max_year_built=('Year built','max'))
classification_summary.head(10)


# %matplotlib inline
import matplotlib.pyplot as plt

# Plot
# Plot two or more columns in your data using matplotlib, seaborn, or plotly. Make sure that your plot has labels, a title, a grid, and a legend.
# Ploting data related of amount of apartment associating with min_year_built and max_year_built
fig, ax = plt.subplots()
ax.scatter(classification_summary['apt_count'],
            classification_summary['min_year_built'],
            label='min_year_built')
ax.scatter(classification_summary['apt_count'],
            classification_summary['max_year_built'],
            label='max_year_built')
ax.set_title('Year Build vs Amount of Apartment')
ax.set_xlabel('Amount of Apartment')
ax.set_ylabel('Year Buld')
ax.set_axisbelow(True)
ax.grid(alpha=0.8)
ax.legend()

plt.savefig('filename.png')
