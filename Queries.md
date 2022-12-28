# Query

## Predicate Pushdown
Because we use ORC which has good support for ppd.

```
set hive.optimize.ppd = true;
```

## query
1. How many customers used special promo code which have no conditions and get discount for new users during sign in, compare last two subsequent years.
    ```sql
    with t as (
        select promo_code_id
        from promo_code_ext
        where category_discount.conditions is null and is_used = true
    )
    select year, count(year) as user_cnt
    from signing_in_ext, t
    where year = 2021 and signing_in_ext.promo_code_id = t.promo_code_id
    GROUP BY year
    UNION ALL
    select year, count(year) as user_cnt
    from signing_in_ext, t
    where year = 2020 and signing_in_ext.promo_code_id = t.promo_code_id
    GROUP BY year;
    ```


2. Compare the number of accounts that were signed-in in later half of year of last two subsequent years.
    ```sql
    with 
        t as (
            select date_id
            from date_par
            where month_no > 6
        )
    select year, count(year) as account_num
    from signing_in_ext, t
    where 
        year = 2021 and
        signing_in_ext.sign_in_date_id = t.date_id
    group by year
    UNION ALL
    select year, count(year) as user_cnt
    from signing_in_ext, t
    where 
        year = 2020 and 
        signing_in_ext.sign_in_date_id = t.date_id
    GROUP BY year;
    ```
   

3. Compare the number of appointments that were set up through web registration in last two subsequent years.
    ```sql
    with 
        t as (
            select junk_booking_id
            from junk_booking_ext
            where 
                channel = "web" and
                is_successful = true
        ),
        y as(
            select date_id, year
            from date_par
            where year = 2021 or year = 2020
        )
    select 2021, count(transaction_no) as appointment_num
    from booking_orc, t, (
            select date_id
            from y
            where year = 2021
        ) as y1
    where 
        booking_orc.junk_booking_id = t.junk_booking_id and
        booking_orc.booking_date_id = y1.date_id
    UNION ALL
    select 2020, count(transaction_no) as appointment_num
    from booking_orc, t, (
            select date_id
            from y
            where year = 2020
        ) as y2
    where 
        booking_orc.junk_booking_id = t.junk_booking_id and
        booking_orc.booking_date_id = y2.date_id;
    ```


4. Compare the number of users who at least twice made an appointment through online registration (compare two subsequent years).
    ```sql
    with 
        t as(
            select junk_booking_id
            from junk_booking_ext
            where 
                channel = "web" and
                is_successful = true
        ),
        y as(
            select date_id, year
            from date_par
            where year = 2021 or year = 2020
        )
    select 2021, count(user_id) as gt2_num
    from(
        select user_id, count(transaction_no) as user_num
        from booking_orc, t, (
                select date_id
                from y
                where year = 2021
            ) as y1
        where
            booking_orc.junk_booking_id = t.junk_booking_id
        group by user_id
    ) as t1 
    where user_num > 2
    union all
    select 2020, count(user_id) as gt2_num
    from(
        select user_id, count(transaction_no) as user_num
        from booking_orc, t, (
                select date_id
                from y
                where year = 2020
            ) as y2
        where
            booking_orc.junk_booking_id = t.junk_booking_id
        group by user_id
    ) as t2
    where user_num > 2;
    ```


5. Compare the proportion of opened web registration sessions (means contact with employee) which end up with making an appointment for two subsequent years.
    ```sql
    with 
        t as (
            select junk_booking_id
            from junk_booking_ext
            where 
                channel = "web" and
                is_successful = true and
                is_employee_needed = true
        ),
        y as(
            select date_id, year
            from date_par
            where year = 2021 or year = 2020
        )
    select 2021, count(transaction_no) as appointment_num
    from booking_orc, t, (
            select date_id
            from y
            where year = 2021
        ) as y1
    where 
        booking_orc.junk_booking_id = t.junk_booking_id and
        booking_orc.booking_date_id = y1.date_id
    UNION ALL
    select 2020, count(transaction_no) as appointment_num
    from booking_orc, t, (
            select date_id
            from y
            where year = 2020
        ) as y2
    where 
        booking_orc.junk_booking_id = t.junk_booking_id and
        booking_orc.booking_date_id = y2.date_id;
    ```


6. What was the people’s average rate (rate = helpful + experience) about our chatting with customer service in two consecutive years?
    ```sql
    with y as (
        select date_id, year
        from date_par
        where year = 2021 or year = 2020
    ) 
    select 2021, avg(rate.helpful + rate.experience) as avg_rate
    from chatting_orc, (
                select date_id
                from y
                where year = 2021
            ) as y1
    where chatting_orc.chat_date_id = y1.date_id
    union all
    select 2020, avg(rate.helpful + rate.experience) as avg_rate
    from chatting_orc, (
                select date_id
                from y
                where year = 2020
            ) as y2
    where chatting_orc.chat_date_id = y2.date_id;
    ```


7. How do people evaluate the average rate (rate = helpfulness + experience) of chatters, compare last two consecutive years of each employee?
    ```sql
    with
        y as (
            select date_id, year
            from date_par
            where year = 2021 or year = 2020
        ) 
    select t1.employee_id, t2.employee_id, t1.avg_rate as rate_2021, t2.avg_rate as rate_2022
    from
    (
        (
            select avg(rate.helpful + rate.experience) as avg_rate, employee_id
            from chatting_orc, (
                select date_id
                from y
                where year = 2021
            ) as y1
            where chatting_orc.chat_date_id = y1.date_id
            group by employee_id
        )as t1
        full outer join
        (
            select avg(rate.helpful + rate.experience) as avg_rate, employee_id
            from chatting_orc, (
                select date_id
                from y
                where year = 2020
            ) as y2
            where chat_date_id = y2.date_id
            group by employee_id
        ) as t2
        on t1.employee_id = t2.employee_id
    );
    ```



8. What was the average response time for the first message in the chat of employees?Sort Jonathan Mitchell, Shannon Santos and Carl Fox.
    ```sql
    select name, avg(response_time) as avg_res
    from chatting_orc, (
        select employee_id, profile.name_and_surname as name
        from employee_ext
        where 
            profile.name_and_surname = "Carl Fox" or 
            profile.name_and_surname = "Jonathan Mitchell" or
            profile.name_and_surname = "Shannon Santos"
    ) as t
    where
        chatting_orc.employee_id = t.employee_id
    group by name
    order by avg_res desc;
    ```


9.  What was the average time of the chats for each employee of female whose average helpful rate and experience rate not less than 3?
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


10. How many cases the customer wanted to call rather than continue chatting for employee from 22-29 years old? 
    ```sql
    select count(chatting_orc.junk_chat_id)
    from chatting_orc, (
        select junk_chat_id 
        from junk_chat_ext
        where 
            is_problem_solved = false and 
            is_call_service_needed = true
    ) as t1,(
        select employee_id
        from employee_ext
        where age_category = "from 22 to 29"
    ) as t2
    where 
        chatting_orc.junk_chat_id = t1.junk_chat_id and
        chatting_orc.employee_id = t2.employee_id;
    ```