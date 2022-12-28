# Report

## External table / static partition

1. I make all tables external which are used for transfer data to the table which store as other type of file.
    **Reason:**
    In real life all this data are created by ERP software or other application. It can easily get new data and convenient for application control it.

2. The table of signing_in is directly static partition in the external table.
   **Reason:**
   Because actually all data related to our topic should output by some applications as time divided. And I prepare data as each year. **For the queries designed which will used this table,  will usually grouped by one year or several months in specific year.**
   So I can partition it mannually by year.

3. I make several tables external which store some data of category.
    **Reason:**
    The data inside is not very big and it may be changed frequently. For example, the table of promo_code which contains information of some promotion code category.

## Internal Table / Dynamic Partion
1. Other tables I use internal table and partition by some other value:
   1. Chatting - partitioned by employee_id
    **Reason:**
    I usually query for employee's performance. It will accelerate query speed when I partition by it. Also in our case, the quantity of employee is much bigger than the year.
   2. Booking - partitioned by junk_book_id (the category of book)
    **Reason:**
    I usually query for different category. It will accelerate query speed when I partition by it.

## Store Type
1. enternal table textfile
   **Reason:**
   They may created by some clerks and may be changed frequently.

2. ORC
   Tables: chatting, booking
   **Reason:**
    * **ORC supports ACID properties.** This two tables are truly related to some transactions: each session of chatting betIen employee and customers, customer's booking for meeting
    * **ORC is more efficient in compression**, and data for these tables will be large
    * **ORC is more capable of predicate pushdown.** In our queries I may use this properties. **e.g.:**
        **What was the average time of the chats for each employee of female whose average helpful rate and experience rate not less than 3?**
        Here I use left join and use some conditions on right part which can predicate pushdown.
        ```sql
        select t1.employee_id as employee_id, sex as sex, avg_duration as avg_duration
        from 
        (
            select employee_id, profile.sex as sex
            from employee_ext
            where profile.sex = 'F'
        ) as t1
        LEFT JOIN    
        (
            select 
                employee_id,
                avg(duration) as avg_duration,
                avg(rate.helpful) as avg_helpful, 
                avg(rate.experience) as avg_experience
            from chatting_orc
            GROUP BY employee_id
        ) as t2
        ON 
            t2.avg_helpful >= 3 and 
            t2.avg_experience >= 3 and 
            t1.employee_id = t2.employee_id
        where avg_duration is not NULL;
    ```

3. parquet
   Tables: date, time, user
   **Reason:**
   1. **Parquet is much better for analytical queries, i.e. reads and queries are much more efficient than writes.** Date, time table can create a long time period in once, so I don't need write in always. Users is because I take the topic into account, it's a large dentist clinic organization which is not a online service, the users maynot change very frequently.
   2. **PARQUET is great for querying subsets of columns in multiple tables.** It is stored in column, and for these tables, I only need one or two columns each query.
   3. **Parquet is better able to store nested data.** There are structs, arrays in these tables.

## Data type
1. Struct
   * Struct of a employee structure which contains some info when do query will use them together.
   * Struct of rate of employee which customers give after service. Because queries usually need calculate a total rate value by all of them.
   * Struct of promotion code info: **category** + **discount** + **conditions**. They are not usually used, the things needed usually is out side the struct. And when need information inside it, they are uesd together. e.g.: How many customers used special promo code which have no **conditions** and get **discount** for new users during **sign in**, compare last two subsequent years.
2. Arrays
   * Array of conditions of one promotion code which embed inside a struct.