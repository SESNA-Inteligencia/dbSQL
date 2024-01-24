USE psociales;



-- -------------------------------------------
-- Crea tabla
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS cmarginacion (
  `id` cmarginacionvarchar(11) NOT NULL,
  `Grado` text NOT NULL,
  `Color` text NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- --------------------------tamprod-----------------
-- Agrega valores a la tabla
-- -------------------------------------------
INSERT INTO psociales.cmarginacion
(id, grado, color) 
VALUES
('01', 'Muy bajo', '#9ecae1'),
('02', 'Bajo', '#6baed6'),
('03', 'Medio', '#4292c6'),
('04', 'Alto', '#2171b5'),
('05', 'Muy Alto', '#084594');