PROCEDURE "W_OMAT"."prd.gops.OMAT::SP_OMAT_INSERT_DMD_CONS_PO" ( ) 
LANGUAGE SQLSCRIPT
	SQL SECURITY DEFINER 
	DEFAULT SCHEMA W_OMAT
	--READS SQL DATA 
	AS
BEGIN
/*------------------------------------------------------------------------------------------------------------------------*
*Program Name : SP_OMAT_INSERT_DMD_CONS_PO
*Project      : Obsolescence Improvement Project
*Developer    : Akhil Martin
*Requestor    : David, Hickman
*Create Date  : 2020/04/17
*--------------------------------------------------------------------------------------------------------------------------*
*Description  : This Stored Procedure identifies the demand, consumption, open order quantity, 
			 	Lam quantity On Hand, Supplier Info and product details of BUY NHAs in OB PR and insert it into
			 	OMAT Tables for OMAT Application.				
*--------------------------------------------------------------------------------------------------------------------------*
Change Log:
Change Id 		Developer		Date			Change Description
---------------------------------------------------------------------------------------------------------------------------*
98687			AKHILJO			2020-04-17		Initial developemnt.
105125			AKHILJO			2020-11-13		Spares consumption query Changed to Subquery Method 
												to solve the issue with "_SYS_BIC"."prd.csbg/CVNS_CSBG_SPARES_DELIVERIES".
106810			AKHILJO			2021-01-26		Fixed Vendor Attribute added & SBM Code.
107767			AKHILJO			2021-03-17		New SP invoking to get the MRP PREFERRED suppliers in all the procurement
												plants in LAM
108422			SRIVAAP         2021-04-14      Added 5 years consumption data for MFG and SPRS
109105			SRIVAAP         2021-05-19      Added filters for MFG and Spares Consumption
113881 			AKHILJO			2021-09-14		Added filter for ZUP INTO Spares Consumption Logic
115167 			AKHILJO			2021-10-20		Removed DISCARD <> 'X' from OPEN PO Qty as this Flag is not using as per Byron
128743          TIWARIS         2023-09-11      added new column restricted all stock and new calculation for LAMQOH
------          BERANSU         2025-02-25      Disconnected Storage location V014 from QOH
*---------------------------------------------------------------------------------------------------------------------------*/	

DECLARE OBPRTCnt INT;
DECLARE NHACnt INT;

OBPRTCnt := 0;
NHACnt := 0;

  NHALIST = Select DISTINCT CMPPRTNO, NHA FROM "_SYS_BIC"."prd.gops.OMAT/CV_OMAT_OBPN_BUY_NHA_DTLS" WHERE "ISPROCESSED" = 'N' ;
  
--Get the Manufacturing weekly consumption for last 3 years.
 --------------
 -- Step 1.
 --------------

	MFGCONS = SELECT  DISTINCT A.CMPPRTNO,
            	              B.MATNR,
                              SUM(CASE WHEN B.SHKZG = 'S' THEN -1*B.MENGE ELSE B.MENGE END) AS "MFG_3YRS_CONSUMPTION"
               FROM "_SYS_BIC"."prd.global.ecc/CV_MSEG" B 
               INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_MAKT" C ON C.MATNR = B.MATNR 
               INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_MKPF" D ON B.MBLNR = D.MBLNR 
               INNER JOIN :NHALIST A ON A.NHA = B.MATNR
               WHERE  B.BWART in ('261','262') and D.BLDAT > ADD_DAYS (CURRENT_DATE, -365*3)
                              AND D.BLDAT< CURRENT_DATE
               GROUP BY A.CMPPRTNO, B.MATNR, C.MAKTX;                 
                        
 --Get the Manufacturing weekly DEMAND for 26 WEEKS.
 ---------------
 -- Step 2.
 ---------------
                        
  MFGDMD = 	SELECT DISTINCT B.CMPPRTNO,		
						MATNR, 
                        SUM(DMDPD + DMD1 + DMD2 + DMD3 + DMD4 + DMD5 + DMD6 + DMD7 + DMD8 + DMD9 + DMD10 + DMD11 + DMD12 + DMD13 + DMD14
                         + DMD15 + DMD16 + DMD17 + DMD18 + DMD19 + DMD20 + DMD21 + DMD22 + DMD23 + DMD24 + DMD25 + DMD26) AS "MFG_DMD"       
                  FROM "_SYS_BIC"."prd.global.ecc/CV_ZMMFORECAST" A
                  INNER JOIN :NHALIST B ON B.NHA = A.MATNR
                  WHERE ROWTYPE = '06'
                        AND WERKS = 'COMB'
                  GROUP BY B.CMPPRTNO, MATNR;
 
  --Get the SPARES weekly DEMAND for 26 WEEKS.
  --------------
  -- Step 3.
  --------------

	SPRSDMD	= 	SELECT DISTINCT B.CMPPRTNO,		
						MATNR,
                        SUM(DMDPD + DMD1 + DMD2 + DMD3 + DMD4 + DMD5 + DMD6 + DMD7 + DMD8 + DMD9 + DMD10 + DMD11 + DMD12 + DMD13 + DMD14
                         + DMD15 + DMD16 + DMD17 + DMD18 + DMD19 + DMD20 + DMD21 + DMD22 + DMD23 + DMD24 + DMD25 + DMD26) AS "SPRS_DMD"
                  FROM "_SYS_BIC"."prd.global.ecc/CV_ZMMFORECAST" A
                  INNER JOIN :NHALIST B ON B.NHA = A.MATNR
                  WHERE ROWTYPE = '07'
                        AND WERKS = 'COMB'
                  GROUP BY B.CMPPRTNO, MATNR;
                  
  --Get the SPARES weekly CONSUMPTION for LAST 3 YEARS.
  --------------
  -- Step 4.
  --------------

    --105125 START Spares consumption query data fetching Changed to Table to solve the issue with the View "_SYS_BIC"."prd.csbg/CVNS_CSBG_SPARES_DELIVERIES". 

	SPRSCONS =  SELECT B.CMPPRTNO, A.MATERIAL,  SUM(A.QTY_SHIPPED) AS "SPRS_3YRS_CONSUMPTION" 
				FROM "ALEX_CUSTOM"."ZT_CSBG_SPARES_DELIVERIES" A
				INNER JOIN :NHALIST B ON B.NHA = A.MATERIAL
				WHERE A.SOURCE_DOC <> 'STO' 
						AND A.ORDER_TYPE IN ('TA','ZCON','ZO09','ZO04','ZSD','ZSO','ZFO','ZUP') 
						AND A.ACTUAL_GI > ADD_DAYS (CURRENT_DATE, -365*3) 
				GROUP BY A.MATERIAL, B.CMPPRTNO
				ORDER BY A.MATERIAL ASC;  

	--105125 END Spares consumption query data fetching Changed to Table to solve the issue with the View "_SYS_BIC"."prd.csbg/CVNS_CSBG_SPARES_DELIVERIES".

--Get the Manufacturing weekly consumption for last 12 Months.
 --------------
 -- Step 5.
 --------------
 
	MFG12MNTHSCONS =   SELECT  DISTINCT A.CMPPRTNO,
                              B.MATNR,
                              SUM(CASE WHEN B.SHKZG = 'S' THEN -1*B.MENGE ELSE B.MENGE END) AS "MFG_12MNTHS_CONSUMPTION" --"3 years consumed qty"
                        FROM "_SYS_BIC"."prd.global.ecc/CV_MSEG" B 
                        INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_MAKT" C ON C.MATNR = B.MATNR 
                        INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_MKPF" D ON B.MBLNR = D.MBLNR 
                        INNER JOIN :NHALIST A ON B.MATNR = A.NHA
                        WHERE  B.BWART in ('261','262') and D.BLDAT > ADD_DAYS (CURRENT_DATE, -365)
                              AND D.BLDAT< CURRENT_DATE
                        GROUP BY A.CMPPRTNO, B.MATNR, C.MAKTX;
    
	--Get the LAM Quantity On Hand.
 	--------------
	-- Step 5.
 	--------------                    
 	--128743 Change starts			
 	LAMQOH =    SELECT B.CMPPRTNO,
 						A.MATNR,
 						SUM(LABST +KLABS) AS "LAMQOH",
 						SUM(INSME+UMLME+EINME+SPEME) AS "Restricted_All_Stock"
 				FROM "_SYS_BIC"."prd.global.ecc/CV_MARD" A
 				INNER JOIN :NHALIST B ON A.MATNR = B.NHA
 				WHERE A.LGORT NOT IN('V014')
 				GROUP BY B.CMPPRTNO, A.MATNR;
 	--128743 change end			
 	
	--Get the LAM OPEN PO Quantity.
 	--------------
 	-- Step 5.
 	--------------
			
 	OPENPOQTY = SELECT C.CMPPRTNO,
 						A.MATNR, 
 						SUM(A.MENGE-A.WEMNG) AS "OPEN_PO_QTY"
 				FROM "_SYS_BIC"."prd.global.ecc/CV_ZPO_HSTRY"  A
				INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_EKPO" B ON A.EBELN = B.EBELN AND A.EBELP = B.EBELP
				INNER JOIN :NHALIST C ON A.MATNR = C.NHA 
				WHERE RIGHT(DUE_DT,5) <> '04-01' AND RIGHT(DUE_DT,5) <> '12-31'
						AND A.STATUS <> 'INACT'	 AND TRIM(A.MATNR) <> '' 
						AND TRIM(A.LOEKZ) = ''   --AND A.DISCRD <> 'X'
						AND A.AUSSL <> 'U3'      AND A.BSART <> 'UB'
						AND A.ELIKZ <> 'X'       AND B.PSTYP <> '7' 
						AND B.PSTYP <> '9'
				GROUP BY C.CMPPRTNO, A.MATNR
				ORDER BY A.MATNR ASC ; 		
				
--Get the Manufacturing weekly consumption for last 5 years.
 --------------
 -- Step 6.
 --------------

	MFG5YrsCONS = SELECT  DISTINCT A.CMPPRTNO,
            	              B.MATNR,
                              SUM(CASE WHEN B.SHKZG = 'S' THEN -1*B.MENGE ELSE B.MENGE END) AS "MFG_5YRS_CONSUMPTION"
               FROM "_SYS_BIC"."prd.global.ecc/CV_MSEG" B 
               INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_MAKT" C ON C.MATNR = B.MATNR 
               INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_MKPF" D ON B.MBLNR = D.MBLNR 
               INNER JOIN :NHALIST A ON A.NHA = B.MATNR
               WHERE  B.BWART in ('261','262') and D.BLDAT > ADD_DAYS (CURRENT_DATE, -365*5)
                              AND D.BLDAT< CURRENT_DATE
               GROUP BY A.CMPPRTNO, B.MATNR, C.MAKTX;  
               
  --Get the SPARES weekly CONSUMPTION for LAST 5 YEARS.
  --------------
  -- Step 7.
  --------------

	SPRS5YrsCONS =  SELECT B.CMPPRTNO, A.MATERIAL,  SUM(A.QTY_SHIPPED) AS "SPRS_5YRS_CONSUMPTION" 
				FROM "ALEX_CUSTOM"."ZT_CSBG_SPARES_DELIVERIES" A
				INNER JOIN :NHALIST B ON B.NHA = A.MATERIAL
				WHERE A.SOURCE_DOC <> 'STO' 
						AND A.ORDER_TYPE IN ('TA','ZCON','ZO09','ZO04','ZSD','ZSO','ZFO','ZUP') 
						AND A.ACTUAL_GI > ADD_DAYS (CURRENT_DATE, -365*5) 
				GROUP BY A.MATERIAL, B.CMPPRTNO
				ORDER BY A.MATERIAL ASC;  	
	
	INSERT INTO "W_OMAT"."OMAT_DMD_CONSUMPTION_DTLS"(CMPPRTNO, NHA,"MFG_3YRS_CONSUMPTION","MFG_DMD", "SPRS_DMD", "SPRS_3YRS_CONSUMPTION",
														"MFG_12MNTHS_CONSUMPTION","LAMQOH","RESTRICTED_ALL_STOCK","OPEN_PO_QTY","LAST_MODIFIED_ON","MFG_5YRS_CONSUMPTION","SPRS_5YRS_CONSUMPTION")
	SELECT  DISTINCT
			A.CMPPRTNO,
			A.NHA,
			B."MFG_3YRS_CONSUMPTION",
			C."MFG_DMD",
			D."SPRS_DMD",
			E."SPRS_3YRS_CONSUMPTION",
			F."MFG_12MNTHS_CONSUMPTION",
			G."LAMQOH",
			G."Restricted_All_Stock", --128743 change
			H."OPEN_PO_QTY",
			CURRENT_DATE,
			I."MFG_5YRS_CONSUMPTION",
			J."SPRS_5YRS_CONSUMPTION"
			FROM :NHALIST A
			LEFT JOIN :MFGCONS B 		ON A.NHA = B.MATNR
			LEFT JOIN :MFGDMD C  		ON A.NHA = C.MATNR 
			LEFT JOIN :SPRSDMD D		ON A.NHA = D.MATNR 
			LEFT JOIN :SPRSCONS E 		ON A.NHA = E.MATERIAL
			LEFT JOIN :MFG12MNTHSCONS F	ON A.NHA = F.MATNR
			LEFT JOIN :LAMQOH G			ON A.NHA = G.MATNR
			LEFT JOIN :OPENPOQTY H  	ON A.NHA = H.MATNR
			LEFT JOIN :MFG5YrsCONS I 		ON A.NHA = I.MATNR
			LEFT JOIN :SPRS5YrsCONS J 		ON A.NHA = J.MATERIAL
		WHERE  A.CMPPRTNO = A.NHA
		
				UNION
				
	SELECT  DISTINCT
			A.CMPPRTNO,
			A.NHA,
			B."MFG_3YRS_CONSUMPTION",
			C."MFG_DMD",
			D."SPRS_DMD",
			E."SPRS_3YRS_CONSUMPTION",
			F."MFG_12MNTHS_CONSUMPTION",
			G."LAMQOH",
			G."Restricted_All_Stock",  --128743 change
			H."OPEN_PO_QTY",
			CURRENT_DATE,
			I."MFG_5YRS_CONSUMPTION",
			J."SPRS_5YRS_CONSUMPTION"
			FROM :NHALIST A
			LEFT JOIN :MFGCONS B 		ON A.NHA = B.MATNR
			LEFT JOIN :MFGDMD C  		ON A.NHA = C.MATNR 
			LEFT JOIN :SPRSDMD D		ON A.NHA = D.MATNR 
			LEFT JOIN :SPRSCONS E 		ON A.NHA = E.MATERIAL
			LEFT JOIN :MFG12MNTHSCONS F	ON A.NHA = F.MATNR
			LEFT JOIN :LAMQOH G			ON A.NHA = G.MATNR
			LEFT JOIN :OPENPOQTY H  	ON A.NHA = H.MATNR
			LEFT JOIN :MFG5YrsCONS I 		ON A.NHA = I.MATNR
			LEFT JOIN :SPRS5YrsCONS J 		ON A.NHA = J.MATERIAL
		WHERE  B."MFG_3YRS_CONSUMPTION" > 0 
				OR C."MFG_DMD" > 0 
				OR D."SPRS_DMD" > 0 
				OR E."SPRS_3YRS_CONSUMPTION" > 0 
				OR F."MFG_12MNTHS_CONSUMPTION" > 0 
				OR H."OPEN_PO_QTY" > 0 
				OR I."MFG_5YRS_CONSUMPTION">0
			    OR J."SPRS_5YRS_CONSUMPTION">0;	
				
	MFGCONS   = SELECT * FROM :MFGCONS WHERE 1 = 2;
	MFG5YrsCONS   = SELECT * FROM :MFG5YrsCONS WHERE 1 = 2;
	MFGDMD    = SELECT * FROM :MFGDMD WHERE 1 = 2;
	SPRSDMD   = SELECT * FROM :SPRSDMD WHERE 1 = 2;
	SPRSCONS  = SELECT * FROM :SPRSCONS WHERE 1 = 2;
	SPRS5YrsCONS  = SELECT * FROM :SPRS5YrsCONS WHERE 1 = 2;
	LAMQOH    = SELECT * FROM :LAMQOH WHERE 1 = 2;
	OPENPOQTY = SELECT * FROM :OPENPOQTY WHERE 1 = 2; 
	MFG12MNTHSCONS = SELECT * FROM :MFG12MNTHSCONS WHERE 1 = 2;		
	 			 			
				
	
	--Logging
	
	SELECT COUNT(DISTINCT CMPPRTNO) INTO OBPRTCnt FROM :NHALIST;
	SELECT COUNT(DISTINCT NHA) INTO NHACnt FROM :NHALIST;
	
	INSERT INTO "W_OMAT"."OMAT_LOG_DTLS" VALUES ('SP_OMAT_INSERT_DMD_CONS_PO', 1 , :OBPRTCnt , :NHACnt ,  NOW());
	
END;
