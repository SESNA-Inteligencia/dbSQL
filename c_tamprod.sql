USE psociales;



-- -------------------------------------------
-- Crea tabla
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS ctamprod (
  id varchar(11) NOT NULL,
  tamprod text NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- --------------------------tamprod-----------------
-- Agrega valores a la tabla
-- -------------------------------------------
INSERT INTO psociales.ctamprod
(id, tamprod) 
VALUES
('01', 'Pequeño'),
('02', 'Mediano'),
('03', 'Grande');