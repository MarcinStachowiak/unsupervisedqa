select * from users


--- Review
SELECT q.project_id,q.document_id, q.text, q.created_at, a.answer_category,a.selected_text,d.text,d.unique_document_name, u.email
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
LEFT JOIN users u on a.user_id = u.id
WHERE d.status='done'  and a.created_at>=CAST('2021-03-17' AS DATE) and a.created_at<CAST('2021-04-01' AS DATE)
limit 500

--- Statistics questions
SELECT u.email,to_char(date_trunc('day', a.created_at), 'YYYY-MM-DD'),count(*)
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
LEFT JOIN users u on a.user_id = u.id
WHERE d.status='done' and a.created_at>=CAST('2021-03-15' AS DATE) and a.created_at<CAST('2021-04-01' AS DATE) and a.selected_text is not NULL
GROUP BY 1,2

--- liczba kategorii
SELECT a.answer_category,count(*)
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
LEFT JOIN users u on a.user_id = u.id
WHERE d.status='done' and a.created_at>=CAST('2021-03-15' AS DATE) and a.created_at<CAST('2021-04-01' AS DATE) and a.selected_text is not NULL
GROUP BY 1

SELECT *
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
WHERE a.selected_text is NULL


select * from projects

--- Review
SELECT q.project_id,q.document_id, q.text as question, q.start_offset,q.created_at, a.answer_category,a.selected_text,d.text as context,d.unique_document_name, u.email
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
LEFT JOIN users u on a.user_id = u.id
WHERE d.project_id in (19,20,25,26,27,28) and a.selected_text is not NULL and q.text is not NULL