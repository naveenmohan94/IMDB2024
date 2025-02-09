# step_1 pip installl selenium in command prompot
# step_2 import webdriver and keys from selenium
import streamlit as st
import mysql.connector
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time
import pandas as pd
import re 
import mysql.connector

#data scrapping part

# Initialize WebDriver(based on my use  i am use chrome , based user firefox anything)
driver = webdriver.Chrome()

# IMDb URL for 2024 movies
genres = {"Action":'https://www.imdb.com/search/title/?title_type=feature&release_date=2024-01-01,2024-12-31&genres=action',
          "Adventure" :'https://www.imdb.com/search/title/?title_type=feature&genres=adventure&release_date=2024-01-01,2024-12-31',
          "Comedy" : 'https://www.imdb.com/search/title/?title_type=feature&genres=comedy&release_date=2024-01-01,2024-12-31',
          "Animation":'https://www.imdb.com/search/title/?title_type=feature&genres=animation&release_date=2024-01-01,2024-12-31',
          "Family"   :'https://www.imdb.com/search/title/?title_type=feature&genres=family&release_date=2024-01-01,2024-12-31'
          }
  # Allow page to load

# Function to click "See More" button until all movies are loaded(so i can write the function without arugument )
def load_all_movies():
    while True:# (in this line i am using while loop bcz i dont now range of the page )
        try:
            # Locate the "See More" button
            button = driver.find_element(By.XPATH, '//*[@id="__next"]/main/div[2]/div[3]/section/section/div/section/section/div[2]/div/section/div[2]/div[2]/div[2]/div/span')
            ActionChains(driver).move_to_element(button).perform()
            button.click()
            time.sleep(5)  # Wait for content to load
            print("Loading more movies...")
        except:
            print("No more pages to load.")
            break  # Exit loop when button is not found(i.e page finish)

def movie_data(genre):
    driver.get(genres[genre])  # Open the respective genre page
    time.sleep(5)

    # call the function to Load all movies
    load_all_movies()


    # Extract all movie details
    movies = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div[2]/div[3]/section/section/div/section/section/div[2]/div/section/div[2]/div[2]/ul/li')


#creating the list for each category
    titles = []
    ratings = []
    votings = []
    durations = []
    for i in movies :#using for loop to iterate the data from movies(variable)
        try:
            title_data = i.find_element(By.XPATH, './div/div/div/div[1]/div[2]/div[1]/a/h3').text
            rating_data = i.find_element(By.XPATH, './div/div/div/div[1]/div[2]/span/div/span/span[1]').text
            voting_data = i.find_element(By.XPATH, './div/div/div/div[1]/div[2]/span/div/span/span[2]').text
            duration_data = i.find_element(By.XPATH, './div/div/div/div[1]/div[2]/div[2]/span[2]').text
        # to add(append) the text data for each list 
            titles.append(title_data)
            ratings.append(rating_data)
            votings.append(voting_data)
            durations.append(duration_data)

        except Exception as M :
            print(f"some of movies data is :{M}")
            continue
    # in this line return function and extract the data categeroy wise with dataframe
    return pd.DataFrame({
        "Genre": genre,
        "Titles": titles,
        "Ratings": ratings,
        "Votings": votings,
        "Duration": durations
    })
# in this line i will store the data for each genre wise 
action = movie_data("Action")
adventure = movie_data("Adventure")
comedy = movie_data("Comedy")
animation = movie_data("Animation")
family = movie_data('Family')


# Combine all genre DataFrames into one
final_df = pd.concat([action, adventure, comedy,animation,family], ignore_index=True)

#  Save DataFrame to CSV
final_df.to_csv("IMDb_2024_Movies.csv", index=False)

# Close WebDriver
driver.quit()

# get the csv files

d = pd.read_csv("\\Users\\NAVEEN\\OneDrive\\Desktop\\project\\IMDb_2024_Movies.csv")

# write the function to remove(int,.) in Title cloumn(using text_regression method )

def tiles_integer_remove(x):
    return re.sub(r"[\d{3}+.]","",x).strip()

#Apply the funtion in Titles cloumn
d['Titles'] = d['Titles'].apply(tiles_integer_remove)

#A Write the function for removing the "()" in Votings column(using text_regression method )
def votings_K_remove(x):
    return re.sub(r"[()]","",x)

#Apply the funtion in Titles cloumn
d["Votings"]= d["Votings"].apply(votings_K_remove)

#converting the object(string)values to float 
d["Votings"]= d["Votings"].str.replace("K","1000").astype(float)

# write the function to remove h and m in duration cloumn(using text_regression method)
def remove_h_m_duration(x): 
    return re.sub(r"[a-zA-z]","",x)
#Apply the function in Duration cloumn
d["Duration"] = d["Duration"].apply(remove_h_m_duration)

#After removing the h and m in duration cloumn we have some values

# In duration cloumn we have value 16+ , so we replace with 16
d["Duration"] = d["Duration"].str.replace("16+","16")

# In duration cloumn we have value - , so we replace with 0
d["Duration"] = d["Duration"].str.replace("-","0")



# Function to Convert to Seconds
def convert_to_seconds(duration):
    parts = duration.split()  # Split by space
    
    # Set default values for hours and minutes
    hours = 0
    minutes = 0

    if len(parts) == 2:  # Case: "2 28" (Both Hours and Minutes)
        hours, minutes = int(parts[0]), int(parts[1])
    elif len(parts) == 1:  # Case: "50" or "1" (Only Minutes)
        minutes = int(parts[0])
    
    return (hours * 3600) + (minutes * 60)  # Convert to total seconds

# Apply Function
d['DUR_in_seconds'] = d['Duration'].apply(convert_to_seconds)

#to drop the duration cloumn
d.drop(columns="Duration",axis=1,inplace=True)

# collect the values of dataframe and to list to store value in database(mysql)
insert_values = d.values.tolist()

# datawarehousing part

# connection vscode to mysql(database)
try:

    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Naveen@090194"
    )

    mycursor = mydb.cursor()
    # creating the database in mysql
    mycursor.execute("CREATE DATABASE IMDB2024")
    print("databse succesfully created in mysql")
except :
    print("print the database already created")


#create the table for in database(IMDB2024)

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Naveen@090194",
  database="IMDB2024"
)

mycursor = mydb.cursor()

try :
      
    create_query = ('''CREATE TABLE movies (        Genre VARCHAR(255), 
                                                        Titles VARCHAR(255),
                                                        Ratings float,
                                                        Votings float,
                                                        DUR_in_seconds float 
                                                        )''')
    mycursor.execute(create_query)

    print("TABLES ARE SUCCESFUULY CREATED IN MYSQL")
except :
  print("TABLES ALREADY  IN MYSQL")

#INSERT THE VALUE IN TABLE ON (IMDB20240)DATABASE

try:

    insert_query = ''' insert into movies
                     values
                     (%s,%s,%s,%s,%s)'''
    mycursor.executemany(insert_query,insert_values)   
    mydb.commit()
    print("values are successfully inserted in the table")
except :
    print("values are already exist")





# Streamlit UI
st.title("🎬 IMDB 2024 PROJECT")
st.image("C:/Users/NAVEEN/OneDrive/Desktop/image/imdb2024.webp", width=800)
st.sidebar.header("Choose the Page")
page = st.sidebar.radio("Select Page", ["VISUALIZATION", "MYSQL","FILTER OPTIONS"]) 

# MySQL Connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Naveen@090194",
    database="IMDB2024"
)
cursor = conn.cursor()

# Query Dictionary
queries = {
    "MOVIES FOR HIGH RATING AND VOTINGS": """
        SELECT Titles, Genre, Ratings, Votings
        FROM movies
        WHERE Ratings IS NOT NULL AND Votings IS NOT NULL
        ORDER BY Ratings DESC, Votings DESC
        LIMIT 10;
    """,
    "COUNT OF MOVIES FOR EACH GENRE": """
       SELECT  GENRE,count(TITLES)AS TOTAL_MOVIES 
    FROM MOVIES 
    GROUP BY GENRE ORDER BY TOTAL_MOVIES DESC 
        ;
    """,
    "AVG-DURATION FOR EACH GENRE": """
           SELECT GENRE , avg(DUR_in_seconds) AS DURATION_AVG
    FROM MOVIES GROUP BY GENRE
    ORDER BY DURATION_AVG DESC;
    """,
    "AVG_VOTING COUNTS FOR EACH GENRE" : """ SELECT GENRE , AVG(Votings) AS AVG_VOTINGS 
    FROM MOVIES GROUP BY GENRE ORDER BY   AVG_VOTINGS DESC ;
    """,
    "RATINGS FOR ALL MOVIES" :"""SELECT Titles , Ratings FROM MOVIES;""",
    "TOP RATED MOVIES FOR EACH GENRE" :"""SELECT m.Genre, m.Titles, m.Ratings
FROM movies m
INNER JOIN (
    SELECT Genre, MAX(Ratings) AS MaxRating
    FROM movies
    GROUP BY Genre
) AS max_rated
ON m.Genre = max_rated.Genre AND m.Ratings = max_rated.MaxRating;""",
"HIGHEST VOTING_COUNT FOR GENRE" :"""SELECT GENRE,COUNT(VOTINGS) AS TOTAL_COUNTS FROM MOVIES
GROUP BY GENRE
ORDER BY TOTAL_COUNTS DESC;""",
"LONGEST MOVIE DURATION":"""SELECT TITLES , MAX(DUR_in_seconds) AS LONGEST_MOVIE_DURATION FROM MOVIES
GROUP BY TITLES
ORDER BY  LONGEST_MOVIE_DURATION  DESC
limit 10 ;""",
"SHORTEST MOVIE DURATION" :"""
SELECT TITLES, MIN(DUR_in_seconds) AS SHORTEST_MOVIE
FROM MOVIES
WHERE DUR_in_seconds > 0
GROUP BY TITLES
ORDER BY SHORTEST_MOVIE ASC
LIMIT 10;""",
"AVERAGE RATINGS FOR GENRE_WISE":""" SELECT  GENRE, AVG(RATINGS)AS AVG_RATINGS 
 FROM MOVIES
 GROUP BY GENRE;""",
 "COUNT FOR RATINGS AND VOTINGS" : """SELECT Ratings, Votings
FROM Movies
WHERE Ratings IS NOT NULL AND Votings IS NOT NULL
;"""
}

# MYSQL Page Logic
if page == "MYSQL":
    query_choice = st.sidebar.selectbox("Select Query", ["MOVIES FOR HIGH RATING AND VOTINGS",
                                                          "COUNT OF MOVIES FOR EACH GENRE",
                                                          "AVG-DURATION FOR EACH GENRE",
                                                          "AVG_VOTING COUNTS FOR EACH GENRE",
                                                          "RATINGS FOR ALL MOVIES",
                                                          "TOP RATED MOVIES FOR EACH GENRE",
                                                          "HIGHEST VOTING_COUNT FOR GENRE",
                                                          "LONGEST MOVIE DURATION",
                                                          "SHORTEST MOVIE DURATION",
                                                          "AVERAGE RATINGS FOR GENRE_WISE",
                                                          "COUNT FOR RATINGS AND VOTINGS"

                                                          ]
    )

    # Execute the correct SQL query
    if query_choice:
        try:
            cursor.execute(queries[query_choice])  # Now passing actual SQL query
            result = cursor.fetchall()

            # Convert to DataFrame for display
            if query_choice == "MOVIES FOR HIGH RATING AND VOTINGS":
                st.markdown("<h1 style='color: blue;'>MOVIES FOR HIGH RATING AND VOTINGS</h1>", unsafe_allow_html=True)
                df_1 = pd.DataFrame(result, columns=["Title", "Genre", "Ratings", "Votings"])
                st.dataframe(df_1)
            elif query_choice == "COUNT OF MOVIES FOR EACH GENRE":
                st.markdown("<h1 style='color: blue;'>COUNT OF MOVIES FOR EACH GENRE</h1>", unsafe_allow_html=True)
                df_2 = pd.DataFrame(result, columns=["Genre", "TOTAL_MOVIES "])
                st.dataframe(df_2)
            elif query_choice == "AVG-DURATION FOR EACH GENRE":
                st.markdown("<h1 style='color: blue;'>AVG-DURATION FOR EACH GENRE</h1>", unsafe_allow_html=True)
                df_3 = pd.DataFrame(result, columns=["GENRE", "DURATION_AVG"])
                
    
                # Conversion Function
                def convert_to_hours_minutes(seconds):
                    hours = int(seconds) // 3600
                    minutes = (int(seconds) % 3600) // 60
                    return f"{hours}h {minutes}m"
                
                # Apply Conversion
                df_3['DURATION_HOURS_MIN'] = df_3['DURATION_AVG'].apply(convert_to_hours_minutes)
                st.dataframe(df_3)
                
            elif query_choice =="AVG_VOTING COUNTS FOR EACH GENRE" :
                  st.markdown("<h1 style='color: blue;'>AVG_VOTING COUNTS FOR EACH GENRE</h1>", unsafe_allow_html=True)
                  df_4 = pd.DataFrame(result, columns=["Genre", "AVG_VOTINGS"])
                  st.dataframe(df_4)
            elif query_choice == "RATINGS FOR ALL MOVIES" :
                  st.markdown("<h1 style='color: blue;'>RATINGS FOR ALL MOVIES</h1>", unsafe_allow_html=True)
                  df_5 = pd.DataFrame(result, columns=['Titles' , 'Ratings'])
                  st.dataframe(df_5)
            elif query_choice == "TOP RATED MOVIES FOR EACH GENRE" :
                  st.markdown("<h1 style='color: blue;'>TOP RATED MOVIES FOR EACH GENRE</h1>", unsafe_allow_html=True)
                  df_6 = pd.DataFrame(result, columns=['GENRE','TITLES','RATINGS'])
                  st.dataframe(df_6)
            elif query_choice == "HIGHEST VOTING_COUNT FOR GENRE" :
                  st.markdown("<h1 style='color: blue;'>HIGHEST VOTING_COUNT FOR GENRE</h1>", unsafe_allow_html=True)
                  df_7 = pd.DataFrame(result, columns=['GENRE','TOTAL_COUNTS',])
                  st.dataframe(df_7)
            elif query_choice == "LONGEST MOVIE DURATION" :
                  st.markdown("<h1 style='color: blue;'>LONGEST MOVIE DURATION</h1>", unsafe_allow_html=True)
                  df_8 = pd.DataFrame(result, columns=['TITLES','LONGEST_MOVIE_DURATION',])
                  
                  def convert_to_hours_minutes(seconds):
                     hours = int(seconds) // 3600
                     minutes = (int(seconds) % 3600) // 60
                     return f"{hours}h {minutes}m"
                  df_8['LONGEST_MOVIE_HOURS_MINS'] = df_8['LONGEST_MOVIE_DURATION'].apply(convert_to_hours_minutes)
                  st.dataframe(df_8)
            elif query_choice == "SHORTEST MOVIE DURATION"  :
                  st.markdown("<h1 style='color: blue;'>SHORTEST MOVIE DURATION</h1>", unsafe_allow_html=True)
                  df_9 = pd.DataFrame(result, columns=['TITLES','SHORTEST_MOVIE_DURATION',])
                  def convert_to_hours_minutes(seconds):
                     hours = int(seconds) // 3600
                     minutes = (int(seconds) % 3600) // 60
                     return f"{hours}h {minutes}m"
                  df_9['SHORTEST_MOVIE_HOURS_MINS'] = df_9['SHORTEST_MOVIE_DURATION'].apply(convert_to_hours_minutes)
                  st.dataframe(df_9)
            elif query_choice ==  "AVERAGE RATINGS FOR GENRE_WISE" :
                  st.markdown("<h1 style='color: blue;'>AVERAGE RATINGS FOR GENRE_WISE</h1>", unsafe_allow_html=True)
                  df_10 = pd.DataFrame(result, columns=["GENRE", "AVG_RATINGS"])
                  st.dataframe(df_10)
            elif query_choice ==   "COUNT FOR RATINGS AND VOTINGS" :
                  st.markdown("<h1 style='color: blue;'>COUNT FOR RATINGS AND VOTINGS</h1>", unsafe_allow_html=True)
                  df_11 = pd.DataFrame(result, columns=["RATINGS_COUNT", "VOTINGS_cOUNT"])
                  st.dataframe(df_11)
            


            #st.dataframe(df)

        except Exception as e:
            st.error(f"Error: {e}")

# VISUALIZATION Page Logic

if page == "VISUALIZATION":
    query_choice = st.sidebar.selectbox("Select Query", ["MOVIES FOR HIGH RATING AND VOTINGS",
                                                          "COUNT OF MOVIES FOR EACH GENRE",
                                                          "AVG-DURATION FOR EACH GENRE",
                                                          "AVG_VOTING COUNTS FOR EACH GENRE",
                                                          "RATINGS FOR ALL MOVIES",
                                                          "TOP RATED MOVIES FOR EACH GENRE",
                                                          "HIGHEST VOTING_COUNT FOR GENRE",
                                                          "LONGEST MOVIE DURATION",
                                                          "SHORTEST MOVIE DURATION",
                                                          "AVERAGE RATINGS FOR GENRE_WISE",
                                                          "COUNT FOR RATINGS AND VOTINGS"

                                                          ]



    )

    
    if query_choice:

        try:
            cursor.execute(queries[query_choice])  # Now passing actual SQL query
            result = cursor.fetchall()

            # Convert to DataFrame for display
            if query_choice == "MOVIES FOR HIGH RATING AND VOTINGS":
                st.markdown("<h1 style='color: blue;'>MOVIES FOR HIGH RATING AND VOTINGS</h1>", unsafe_allow_html=True)
                df_1 = pd.DataFrame(result, columns=["Title", "Genre", "Ratings", "Votings"])

            # Plotting
                #plt.figure(figsize=(10, 6))
                #sns.scatterplot(data = df_1,
                #x = "Votings" ,
                #y = "Ratings",
                #size ="Genre",
                #hue = "Title")
                sns.barplot(data = df_1, x = 'Ratings', y = 'Votings', hue = "Genre", palette= 'viridis')
                plt.title('BARPLOT OF RATINGS VS. VOTING_COUNTS')
                plt.xlabel('Ratings')
                plt.ylabel('Voting Counts')
                plt.show()
                # Display plot in Streamlit
                st.pyplot(plt)

            elif query_choice == "COUNT OF MOVIES FOR EACH GENRE":
                 st.markdown("<h1 style='color: blue;'>COUNT OF MOVIES FOR EACH GENRE</h1>", unsafe_allow_html=True)
                 df_2 = pd.DataFrame(result, columns=["Genre", "TOTAL_MOVIES "])

                    # Plotting
                 plt.figure(figsize=(10, 6))
                 sns.barplot(data = df_2, x = "TOTAL_MOVIES ", y = "Genre", palette= 'viridis')
                 plt.title('BARTPLOT of GENRE VS. TOTAL_MOVIES')
                 plt.xlabel('GENRE')
                 plt.ylabel('TOTAL_MOVIES')
                 plt.show()
                # Display plot in Streamlit
                 st.pyplot(plt)
            
            elif query_choice == "AVG-DURATION FOR EACH GENRE":
                st.markdown("<h1 style='color: blue;'>AVG-DURATION FOR EACH GENRE</h1>", unsafe_allow_html=True)
                df_3 = pd.DataFrame(result, columns=["GENRE", "DURATION_AVG"])
                
    
                # Conversion Function
                def convert_to_hours_minutes(seconds):
                    hours = int(seconds) // 3600
                    minutes = (int(seconds) % 3600) // 60
                    return f"{hours}h {minutes}m"
                
                # Apply Conversion
                df_3['DURATION_HOURS_MIN'] = df_3['DURATION_AVG'].apply(convert_to_hours_minutes)
                
               
                 # Plotting
                plt.figure(figsize=(10, 6))

                    # Horizontal bar plot
                plt.barh(df_3['GENRE'], df_3['DURATION_AVG'], color='GREEN')

                    # Adding labels
                plt.xlabel('Average Duration (Minutes)')
                plt.ylabel('Genre')
                plt.title('Average Movie Duration by Genre')

                    # Adding data labels (showing hours and minutes)
                for index, value in enumerate(df_3['DURATION_AVG']):
                    plt.text(value, index, df_3['DURATION_HOURS_MIN'][index], va='center', ha='left', fontsize=10, color='black')
                    

                    # Adjust layout
                    plt.tight_layout()

                    # Display the plot
                plt.show()
                 # Display plot in Streamlit
                st.pyplot(plt)
            elif query_choice == "AVG_VOTING COUNTS FOR EACH GENRE":
                st.markdown("<h1 style='color: green;'>AVG_VOTING COUNTS FOR EACH GENRE</h1>", unsafe_allow_html=True)
                
                # Assuming 'result' contains the voting data
                df_4 = pd.DataFrame(result, columns=["GENRE", "AVG_VOTINGS"])

                
                plt.figure(figsize=(10, 6))

                # Horizontal bar plot
                plt.barh(df_4['GENRE'], df_4['AVG_VOTINGS'], color='orange')

                # Adding labels
                plt.xlabel('Average Voting Counts')
                plt.ylabel('Genre')
                plt.title('Average Voting Counts by Genre')

                # Adding data labels
                for index, value in enumerate(df_4['AVG_VOTINGS']):
                    plt.text(value, index, str(value), va='center', ha='left', fontsize=10, color='black')

                # Adjust layout
                plt.tight_layout()

                # Display the plot using Streamlit
                st.pyplot(plt)
            elif query_choice == "RATINGS FOR ALL MOVIES":
                    st.markdown("<h1 style='color: purple;'>RATINGS FOR ALL MOVIES</h1>", unsafe_allow_html=True)
                    
                    # Assuming 'result' contains the rating data
                    df_5 = pd.DataFrame(result, columns=['Titles' , 'RATINGS'])

                    # Clear previous plots
                    plt.clf()
                    fig, axes = plt.subplots(1, 2, figsize=(14, 6))  # Two plots side-by-side

                    # Histogram
                    axes[0].hist(df_5['RATINGS'], bins=10, color='skyblue', edgecolor='black')
                    axes[0].set_title('Histogram of Movie Ratings')
                    axes[0].set_xlabel('Ratings')
                    axes[0].set_ylabel('Frequency')

                    # Boxplot
                    axes[1].boxplot(df_5['RATINGS'], vert=True, patch_artist=True,
                                    boxprops=dict(facecolor='lightgreen', color='black'),
                                    medianprops=dict(color='red', linewidth=2))
                    axes[1].set_title('Boxplot of Movie Ratings')
                    axes[1].set_ylabel('Ratings')

                    # Adjust layout
                    plt.tight_layout()

                    # Display the plots using Streamlit
                    st.pyplot(fig)
                    # Optional: Display the raw data for reference
                    with st.expander("📋 View Ratings Data"):
                     st.dataframe(df_5)
            
            elif query_choice == "TOP RATED MOVIES FOR EACH GENRE" :
                    st.markdown("<h1 style='color: blue;'>TOP RATED MOVIES FOR EACH GENRE</h1>", unsafe_allow_html=True)
                    df_6 = pd.DataFrame(result, columns=['GENRE','TITLES','RATINGS'])
                  # Sorting for better visual clarity
                    df_sorted = df_6.sort_values(by='RATINGS', ascending=True)

                    # Plotting Horizontal Bar Chart
                    plt.figure(figsize=(10, 6))
                    bars = plt.barh(df_sorted['GENRE'], df_sorted['RATINGS'], color='skyblue', edgecolor='black')

                    # Adding Movie Titles and Ratings as Labels on Bars
                    for bar, title, rating in zip(bars, df_sorted['TITLES'], df_sorted['RATINGS']):
                        plt.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                                f"{title} ({rating})", va='center', fontsize=9, color='black')

                    # Labels and Title
                    plt.xlabel('Ratings')
                    plt.ylabel('Genre')
                    plt.title('Top-Rated Movies by Genre', fontsize=14, color='purple')

                    # Adjust Layout
                    plt.tight_layout()

                    # Display in Streamlit
                    st.pyplot(plt)
            elif query_choice == "HIGHEST VOTING_COUNT FOR GENRE" :
                  st.markdown("<h1 style='color: blue;'>HIGHEST VOTING_COUNT FOR GENRE</h1>", unsafe_allow_html=True)
                  df_7 = pd.DataFrame(result, columns=['GENRE','TOTAL_COUNTS',])
                  # Plotting the Pie Chart
                  plt.figure(figsize=(8, 8))
                  colors = plt.cm.Paired.colors  # Using a colormap for diverse colors

                # Pie chart with percentages and explosion effect for the top genre
                  explode = [0.1 if i == df_7['TOTAL_COUNTS'].idxmax() else 0 for i in range(len(df_7))]

                  plt.pie(df_7['TOTAL_COUNTS'], 
                        labels=df_7['GENRE'], 
                        autopct='%1.1f%%', 
                        startangle=140, 
                        colors=colors, 
                        explode=explode, 
                        shadow=True)

                # Adding a Title
                  plt.title('Most Popular Genres by Voting Counts', fontsize=16, color='purple')

                # Display in Streamlit
                  st.pyplot(plt)
            elif query_choice == "LONGEST MOVIE DURATION" :
                st.markdown("<h1 style='color: blue;'>LONGEST MOVIE DURATION</h1>", unsafe_allow_html=True)
                df_8 = pd.DataFrame(result, columns=['TITLES','LONGEST_MOVIE_DURATION',])
                  
                def convert_to_hours_minutes(seconds):
                     hours = int(seconds) // 3600
                     minutes = (int(seconds) % 3600) // 60
                     return f"{hours}h {minutes}m"
                df_8['LONGEST_MOVIE_HOURS_MINS'] = df_8['LONGEST_MOVIE_DURATION'].apply(convert_to_hours_minutes)
                
                    # Plotting Horizontal Bar Chart
                plt.figure(figsize=(10, 6))
                plt.barh(df_8['TITLES'], df_8['LONGEST_MOVIE_DURATION'], color='teal')
                plt.xlabel('Duration (in Minutes)')
                plt.title('Top 10 Longest Movies')

                    # Adding Correct Labels (Hours & Minutes)
                for index, value in enumerate(df_8['LONGEST_MOVIE_DURATION']):
                        hours = int(value // 3600)
                        minutes = int((value % 3600)//60)
                        plt.text(value + 100, index, f'{hours}h {minutes}m', va='center', fontsize=9)

                    # Inverting Y-axis for better readability
                plt.gca().invert_yaxis()

                    # Display in Streamlit
                st.pyplot(plt)
            elif query_choice == "SHORTEST MOVIE DURATION"  :
                st.markdown("<h1 style='color: blue;'>SHORTEST MOVIE DURATION</h1>", unsafe_allow_html=True)
                df_9 = pd.DataFrame(result, columns=['TITLES','SHORTEST_MOVIE_DURATION',])
                def convert_to_hours_minutes(seconds):
                     hours = int(seconds) // 3600
                     minutes = (int(seconds) % 3600) // 60
                     return f"{hours}h {minutes}m"
                df_9['SHORTEST_MOVIE_HOURS_MINS'] = df_9['SHORTEST_MOVIE_DURATION'].apply(convert_to_hours_minutes)
                      # Plotting Horizontal Bar Chart
                plt.figure(figsize=(10, 6))
                plt.barh(df_9['TITLES'], df_9['SHORTEST_MOVIE_DURATION'], color='teal')
                plt.xlabel('Duration (in Minutes)')
                plt.title('Top 10 shortest Movies')

                    # Adding Correct Labels (Hours & Minutes)
                for index, value in enumerate(df_9['SHORTEST_MOVIE_DURATION']):
                     minutes = int(value) // 60  # Convert duration to minutes
                     plt.text(value + 10, index, f'{minutes} mins', va='center', fontsize=9)

                    # Inverting Y-axis for better readability
                plt.gca().invert_yaxis()
                  # Display in Streamlit
                st.pyplot(plt)

            elif query_choice == "AVERAGE RATINGS FOR GENRE_WISE":
                st.markdown("<h1 style='color: blue;'>AVERAGE RATINGS FOR GENRE_WISE</h1>", unsafe_allow_html=True)

    # Convert SQL result to DataFrame
                df_10 = pd.DataFrame(result, columns=["GENRE", "AVG_RATINGS"])

    # Pivoting Data for Heatmap (making GENRE the index)
                heatmap_data = df_10.pivot_table(index="GENRE", values="AVG_RATINGS")

                # Plotting the Heatmap
                plt.figure(figsize=(10, 8))
                sns.heatmap(heatmap_data, annot=True, cmap="coolwarm", fmt=".1f", linewidths=0.5, cbar_kws={"label": "Average Rating"})

                # Customizing Plot Appearance
                plt.title("Average Ratings Across Genres", fontsize=16, color="purple")
                plt.xlabel("")   # No label needed for the x-axis
                plt.ylabel("Genres")

                # Display Heatmap in Streamlit
                st.pyplot(plt)

            elif query_choice ==  "COUNT FOR RATINGS AND VOTINGS":
                st.markdown("<h1 style='color: blue;'>CORRELATION ANALYSIS: RATINGS VS VOTING COUNTS</h1>", unsafe_allow_html=True)
    
    # Assuming 'result' contains columns for RATINGS and VOTING_COUNTS
                df_11 = pd.DataFrame(result, columns=["RATINGS_COUNT", "VOTINGS_cOUNT"])
    
    # Plotting Scatter Plot
                plt.figure(figsize=(10, 6))
                plt.scatter(df_11["VOTINGS_cOUNT"], df_11["RATINGS_COUNT"], color='purple', alpha=0.6, edgecolors='w', s=100)
                
                # Adding Labels and Title
                plt.xlabel("Voting Counts", fontsize=12)
                plt.ylabel("Ratings", fontsize=12)
                plt.title("Correlation Between Ratings and Voting Counts", fontsize=14, color='darkblue')
                
                # Adding Grid for Better Readability
                plt.grid(True, linestyle='--', alpha=0.5)
                
                # Display in Streamlit
                st.pyplot(plt)
                correlation = df_11["RATINGS_COUNT"].corr(df_11["VOTINGS_cOUNT"])
                st.write(f"**Correlation Coefficient:** {correlation:.2f}")

        except Exception as e:
             st.error(f"Error: {e}")

# FILTER OPTIONS page logic

if page == "FILTER OPTIONS":
    #title of the page
    st.title("🎬 Interactive Movie Filter")
    try:
        cursor.execute("""SELECT * FROM MOVIES;""")  # Now passing actual SQL query
        result = cursor.fetchall()
        DF = pd.DataFrame(result,columns=["GENRE","TITLES","RATINGS","VOTINGS","DUR_IN_SEC"])
      
        # Convert duration from seconds to hours and minutes
        def convert_to_hours_minutes(seconds):
          hours = int(seconds // 3600)
          minutes = int((seconds % 3600) // 60)
          return f"{hours}h {minutes}m"

        DF['DURATION_HOURS_MINUTES'] = DF["DUR_IN_SEC"].apply(convert_to_hours_minutes)
         
     # Sidebar Filters
        st.sidebar.header("Filter Options")

     # Duration Filter
        duration_filter = st.sidebar.selectbox("Select Duration:", ("All", "< 2 hrs", "2-3 hrs", "> 3 hrs"))

# Ratings Filter
        rating_filter = st.sidebar.slider("IMDb Rating >", min_value=0.0, max_value=10.0, value=0.0, step=0.1)

        # Voting Counts Filter
        voting_filter = st.sidebar.number_input("Voting Counts >", min_value=0, value=0, step=10000)

        # Genre Filter
        genre_options = DF['GENRE'].unique()
        genre_filter = st.sidebar.multiselect("Select Genres:", options=genre_options, default=genre_options)

        # Applying Filters
        filtered_df = DF.copy()

        # Duration Filter Logic
        if duration_filter == "< 2 hrs":
            filtered_df = filtered_df[filtered_df["DUR_IN_SEC"] < 7200]
            st.dataframe( filtered_df)
            st.markdown(f"**Total Movies Found < 2 hrs:** {len(filtered_df)}")

        elif duration_filter == "2-3 hrs":
            filtered_df = filtered_df[(filtered_df["DUR_IN_SEC"] >= 7200) & (filtered_df["DUR_IN_SEC"] <= 10800)]
            st.dataframe( filtered_df)
            st.markdown(f"**Total Movies Found 2-3 hrs:** {len(filtered_df)}")
        elif duration_filter == "> 3 hrs":
            filtered_df = filtered_df[filtered_df["DUR_IN_SEC"] > 10800]
            st.dataframe( filtered_df)
            st.markdown(f"**Total Movies Found> 3 hrs:** {len(filtered_df)}")
        

# Ratings Filter
        filtered_df = filtered_df[filtered_df["RATINGS"] > rating_filter]

# Voting Counts Filter
        filtered_df = filtered_df[filtered_df["VOTINGS"] > voting_filter]
        def format_votes(votes):
    # Remove commas, convert to float first, then to int
         votes = int(float(str(votes).replace(',', '')))  
         return f"{votes // 10000}k"  # Convert to desired 'k' format (e.g., 1891k → 189k)
    # Apply the Formatting Function
        filtered_df['VOTINGS_FORMATTED'] = filtered_df['VOTINGS'].apply(format_votes)

# Genre Filter
        filtered_df = filtered_df[filtered_df['GENRE'].isin(genre_filter)]

# Displaying Filtered Results
        st.markdown("### 🎥 Filtered Movies") 
        st.dataframe(filtered_df[['GENRE', 'TITLES', 'RATINGS', 'VOTINGS_FORMATTED', 'DURATION_HOURS_MINUTES']])

# Summary
        st.markdown(f"**Total Movies Found:** {len(filtered_df)}")
    
    except Exception as e:
         st.error(f"Error: {e}")



# Close Connection
cursor.close()
conn.close()

