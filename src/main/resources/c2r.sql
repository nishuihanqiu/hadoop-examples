SELECT
t_bom.id, t_bom.org_id,
SUBSTRING_INDEX(SUBSTRING_INDEX(t_bom.a_t, ',', t_row.help_topic_id), ',', -1) as regulatory,
REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(t_bom.m_t, ',', t_row.help_topic_id), ',', -1) , '"',"") as material_code
FROM (
SELECT id, org_id,
SUBSTRING(JSON_EXTRACT(`raw_material_list`, "$[*].regulatoryControl") , 2, Length(JSON_EXTRACT(`raw_material_list`, "$[*].regulatoryControl") ) - 2) AS a_t,
SUBSTRING(JSON_EXTRACT(`raw_material_list`, "$[*].materialCode") , 2, Length(JSON_EXTRACT(`raw_material_list`, "$[*].materialCode") ) - 2) AS m_t
FROM ebom WHERE org_id = 2 AND `raw_material_list` IS NOT NULL AND JSON_LENGTH(raw_material_list) != 0
) AS t_bom
JOIN (SELECT (@row_num := @row_num+1) AS help_topic_id FROM material,(select @row_num :=0) as `row` WHERE org_id = 2 ) AS t_row
ON t_row.help_topic_id <= (LENGTH(t_bom.a_t) - LENGTH(REPLACE(t_bom.a_t, ',', '')) + 1) WHERE t_bom.org_id = 2 ORDER BY t_bom.id ASC;

SELECT * FROM
(
SELECT
t_bom.id, t_bom.org_id,
SUBSTRING_INDEX(SUBSTRING_INDEX(t_bom.a_t, ',', t_row.help_topic_id), ',', -1) as regulatory,
REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(t_bom.m_t, ',', t_row.help_topic_id), ',', -1) , '"',"") as material_code
FROM (
SELECT id, org_id,
SUBSTRING(JSON_EXTRACT(`raw_material_list`, "$[*].regulatoryControl") , 2, Length(JSON_EXTRACT(`raw_material_list`, "$[*].regulatoryControl") ) - 2) AS a_t,
SUBSTRING(JSON_EXTRACT(`raw_material_list`, "$[*].materialCode") , 2, Length(JSON_EXTRACT(`raw_material_list`, "$[*].materialCode") ) - 2) AS m_t
FROM ebom WHERE org_id = 2 AND `raw_material_list` IS NOT NULL AND JSON_LENGTH(raw_material_list) != 0
) AS t_bom
JOIN (SELECT (@row_num := @row_num+1) AS help_topic_id FROM material,(select @row_num :=0) as `row` WHERE org_id = 2 ) AS t_row
ON t_row.help_topic_id <= (LENGTH(t_bom.a_t) - LENGTH(REPLACE(t_bom.a_t, ',', '')) + 1) WHERE t_bom.org_id = 2 ORDER BY t_bom.id ASC
) AS result_table WHERE org_id = 2 AND regulatory = 'true'