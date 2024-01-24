USE psociales;



-- -------------------------------------------
-- Crea tabla
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS cproductos (
  id varchar(11) NOT NULL,
  producto text NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- -------------------------------------------
-- Agrega valores a la tabla
-- -------------------------------------------
INSERT INTO psociales.cproductos
(id, producto) 
VALUES
('01', 'Arroz'),
('02', 'Frijol'),
('03', 'Leche'),
('04', 'Maíz'),
('05', 'Trigo');