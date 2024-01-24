USE psociales;

-- Crea tabla a nivel municipal con 
select cve_ent, cve_mun , min(cve_loc) as cve_loc
from inegi
group by cve_ent, cve_mun;
            
-- Crea tabal inegi a nivel municipal con la localidad minima
CREATE TABLE inegi_mun AS
select a.cve_ent, a.cve_mun, a.cve_loc, a.latitud, a.longitud
from inegi as a
INNER JOIN (select cve_ent, cve_mun, min(cve_loc) as cve_loc
		    from inegi
		    group by cve_ent, cve_mun) as b
ON a.cve_ent = b.cve_ent and a.cve_mun = b.cve_mun and a.cve_loc = b.cve_loc;


------------------------------------------------------------------------------------
--  Creación de tabla beneficiarios con laltitud ajustada al centro de localidad 
------------------------------------------------------------------------------------
CREATE TABLE beneficiarios_mun AS
SELECT 
	a.year, 
    a.cultivo, 
    a.tipo, 
    a.cve_ent, 
    a.cve_mun, 
    a.num_beneficiarios, 
    a.pgarantia_total, 
    a.sum_preferencia, 
    a.sum_volincentivado, 
    a.sum_monto_total, 
    b.latitud, 
    b.longitud
    -- c.gm, 
    -- c.imn
FROM inegi_mun as b
LEFT JOIN 
		(SELECT 
			year, 
            cultivo, 
            tipo, 
            cve_ent, 
            cve_mun, 
            count(cve_loc) AS num_beneficiarios, 
            AVG(pgarantia) as pgarantia_total, 
            AVG(preferencia) AS sum_preferencia, 
            sum(volincentivado) AS sum_volincentivado, 
            sum(monto_total) AS sum_monto_total
		FROM beneficiarios
		GROUP BY year, cultivo, tipo, cve_ent, cve_mun) AS a
ON a.cve_ent=b.cve_ent and a.cve_mun=b.cve_mun
-- LEFT JOIN conapo_imm as c
-- ON a.cve_ent=c.cve_ent and a.cve_mun=c.cve_mun
ORDER BY a.year DESC;

--------

SELECT count(*)
FROM beneficiarios;

SELECT count(*)
FROM beneficiarios_mun;
