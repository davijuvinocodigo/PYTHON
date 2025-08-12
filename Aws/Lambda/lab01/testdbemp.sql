-- rdsinframysqldefinitivo.tbv9086_carc_regr_prod_plar definition

CREATE TABLE `tbv9086_carc_regr_prod_plar` (
   `cod_regr_prod_plar` bigint NOT NULL,
   `cod_tipo_carc_espo_prod` smallint NOT NULL,
   `cod_carc_espo_prod_plar` varchar(20) DEFAULT NULL,
   `dat_hor_usua_atui_rgto` datetime NOT NULL,
   `num_funl_cola_cogl_atud` varchar(9) NOT NULL,
   PRIMARY KEY (`cod_regr_prod_plar`, `cod_tipo_carc_espo_prod`),
   KEY `xv90863` (`cod_tipo_carc_espo_prod`),
   CONSTRAINT `fv90861_tbv9088` FOREIGN KEY (`cod_regr_prod_plar`) REFERENCES `tbv9088_regr_prod_plar` (`cod_regr_prod_plar`),
   CONSTRAINT `fv90862_tbv9089` FOREIGN KEY (`cod_tipo_carc_espo_prod`) REFERENCES `tbv9089_tipo_carc_espo_prod` (`cod_tipo_carc_espo_prod`)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 



CREATE TABLE `tbv9088_regr_prod_plar` (
   `cod_regr_prod_plar` bigint NOT NULL,
   `nom_regr_prod_plar` varchar(50) NOT NULL,
   `des_regr_prod_plar` varchar(255) NOT NULL,
   `ind_rgto_ativ` char(1) NOT NULL DEFAULT 'S',
   `dat_hor_inio_vige__regr_prod` datetime NOT NULL,
   `dat_hor_usua_atui_rgto` datetime NOT NULL,
   `num_funl_cola_cogl_atud` varchar(9) NOT NULL,
   PRIMARY KEY (`cod_regr_prod_plar`),
   CONSTRAINT `cv90881_ind_rgto_ativ` CHECK ((`ind_rgto_ativ` in (_utf8mb4'S',_utf8mb4'N')))
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 



CREATE TABLE `tbv9089_tipo_carc_espo_prod` (
   `cod_tipo_carc_espo_prod` smallint NOT NULL,
   `nom_tipo_carc_espo_prod` varchar(50) NOT NULL,
   `des_tipo_carc_espo_prod` varchar(100) DEFAULT NULL,
   PRIMARY KEY (`cod_tipo_carc_espo_prod`)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 


--NÃO CRIAR AGORA
CREATE TABLE `tbv9087_cnfg_regr_prod_plar` (
   `cod_prod_opel` int NOT NULL AUTO_INCREMENT,
   `cod_regr_prod_plar` bigint NOT NULL,
   `dat_hor_usua_atui_rgto` datetime NOT NULL,
   `num_funl_cola_cogl_atud` varchar(9) NOT NULL,
   PRIMARY KEY (`cod_prod_opel`,`cod_regr_prod_plar`),
   KEY `xv90872` (`cod_regr_prod_plar`),
   CONSTRAINT `fv90871_tbv9088` FOREIGN KEY (`cod_regr_prod_plar`) REFERENCES `tbv9088_regr_prod_plar` (`cod_regr_prod_plar`),
   CONSTRAINT `fv90872_tbv9035` FOREIGN KEY (`cod_prod_opel`) REFERENCES `tbv9035_prod_opel` (`cod_prod_opel`)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 

