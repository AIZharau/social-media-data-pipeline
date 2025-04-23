-- Query 1: ТОП-5 самых продаваемых курсов по месяцам
WITH monthly_course_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        course_id,
        COUNT(*) AS sales_count,
        SUM(amount) AS total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('month', order_date) 
            ORDER BY COUNT(*) DESC, SUM(amount) DESC
        ) AS sales_rank
    FROM 
        orders
    WHERE 
        course_id IS NOT NULL
    GROUP BY 
        DATE_TRUNC('month', order_date), course_id
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') AS month,
    c.course_name,
    s.subject_name,
    mcs.sales_count,
    mcs.total_revenue
FROM 
    monthly_course_sales mcs
JOIN 
    courses c ON mcs.course_id = c.course_id
JOIN 
    subjects s ON c.subject_id = s.subject_id
WHERE 
    mcs.sales_rank <= 5 
ORDER BY 
    month DESC, mcs.sales_rank;


-- Оптимизированная версия с использованием partition pruning
SELECT 
    c.course_name,
    s.subject_name,
    COUNT(*) AS sales_count,
    SUM(amount) AS total_revenue
FROM 
    orders
JOIN 
    courses c ON orders.course_id = c.course_id
JOIN 
    subjects s ON c.subject_id = s.subject_id
WHERE 
    order_date >= '2023-01-01' AND order_date < '2023-02-01'
    AND orders.course_id IS NOT NULL
GROUP BY 
    c.course_id, c.course_name, s.subject_name
ORDER BY 
    sales_count DESC, total_revenue DESC
LIMIT 5;

-- Query 2: ТОП-3 самых популярных пакетов по предметам
WITH package_subject_counts AS (
    SELECT 
        s.subject_id,
        s.subject_name,
        o.package_id,
        COUNT(*) AS order_count,
        ROW_NUMBER() OVER (
            PARTITION BY s.subject_id 
            ORDER BY COUNT(*) DESC
        ) AS popularity_rank
    FROM 
        orders o
    JOIN 
        package_courses pc ON o.package_id = pc.package_id
    JOIN 
        courses c ON pc.course_id = c.course_id
    JOIN 
        subjects s ON c.subject_id = s.subject_id
    WHERE 
        o.package_id IS NOT NULL
    GROUP BY 
        s.subject_id, s.subject_name, o.package_id
)
SELECT 
    psc.subject_name,
    p.package_name,
    psc.order_count,
    (
        SELECT COUNT(DISTINCT pc.course_id)
        FROM package_courses pc
        JOIN courses c ON pc.course_id = c.course_id
        WHERE pc.package_id = psc.package_id
        AND c.subject_id = psc.subject_id
    ) AS subject_courses_count
FROM 
    package_subject_counts psc
JOIN 
    packages p ON psc.package_id = p.package_id
WHERE 
    psc.popularity_rank <= 3
ORDER BY 
    psc.subject_name, psc.popularity_rank;
