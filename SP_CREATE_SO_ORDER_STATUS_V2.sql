PROCEDURE "_SYS_BIC"."prd.gops.GLIS::SP_CREATE_SO_ORDER_STATUS_V2" ( I_TAB_VBELN TABLE(VBELN NVARCHAR(10))) 
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER 
	--DEFAULT SCHEMA <default_schema_name>
--	READS SQL DATA 
	AS
BEGIN

/*---------------------------------------------------------------------
*Program Name : SP_CREATE_SO_ORDER_STATUS
*Project      : CV_SQL_SO OPTIMIZATION 
*Developer    : DHARANIDHAR MALLURI
*Requestor    : SANKAR TADINADA
*Create Date  : 15/12/2019
*Call By      : SP_SO_ORDER_STATUS_DELTA_PROCESS , SP_SO_ORDER_STATUS_INIT
*--------------------------------------------------------------------- 
*Description  : TO IMPROVE THE PERFORMANCE OF THE VIEW CV_SQL_SO, WE ARE PRE-CALCULATING
*               RESOURCE INTENSIVE PARTS AND PERSISTING THOSE RESULTS IN THE TABLE ZT_M_SALES_ORDER_STATUS.
*---------------------------------------------------------------------
* Revision Log:
* Date       Developer           Description
* 15/12/2019 MALLUDH             INTIAL VERSION (INCIDENT: INC0462937)
* 26/02/2019 MALLUDH             2ND ITERATION OF PERFORMANCE IMPROVEMENT (INCIDENT: INC0462937)
*----------------------------------------------------------------------*/ 
 
 --DECLARE PAUSE_TIME TIMESTAMP;
 
 CSBG = SELECT  "ORDER_NO"
	       	   ,"ORDER_LINE_NO"
	           ,MAX("CARRIER") AS "CARRIER"
	           ,MAX("CARRIER_NAME") AS "CARRIER_NAME"
	    FROM   "_SYS_BIC"."prd.csbg/CVNS_CSBG_SPARES_DELIVERIES_NEW"
	    WHERE ORDER_NO IN (SELECT VBELN FROM :I_TAB_VBELN) 
	    GROUP BY "ORDER_NO","ORDER_LINE_NO";
	    
 LIKP_LIPS = SELECT  s."VGBEL"
    			  ,s."VGPOS"
    			  ,MAX(s."VBELN") AS "OpenDlvDoc"			 
    			  ,SUM(s."LGMNG") AS "OpenDlvQty"  		 
    			  ,case when MAX(k."WADAT_IST") = '' THEN '0' ELSE SUM(s."LFIMG") END AS "QtyShippedTtLine"	 
    			  ,MAX(k."WADAT_IST")  AS "PGIDate"
    			  ,CASE WHEN MAX(k."WADAT_IST") = '' THEN 'Created'	 
    			        ELSE 'Shipped' 
    			   END  AS "DlvStatus"
   		   FROM "_SYS_BIC"."prd.global.ecc/CVNS_LIPS" s
    	   JOIN "_SYS_BIC"."prd.global.ecc/CVNS_LIKP" k
    	   ON   s."VBELN" = k."VBELN"
    	   WHERE s."VGBEL" <> '' 
    	   AND   s."VGBEL" NOT LIKE '4%' 
    	   AND   s."VGBEL" NOT LIKE '3%'
    	   AND   S."VGBEL" IN (SELECT VBELN FROM :I_TAB_VBELN) 
           AND	 k."WADAT_IST" <> ''
		   GROUP BY s."VGBEL", s."VGPOS";
		   
 SHIPTO = SELECT  v."VBELN"
		         ,v."KUNNR" AS "ShipTo"
		  		 ,k."NAME1" AS "ShipToName"
	  	  FROM   "_SYS_BIC"."prd.global.ecc/CV_VBPA" v 
	  	  JOIN   (SELECT VBELN FROM :I_TAB_VBELN)   IP
	  	  ON     v.VBELN = IP.VBELN
	  	  JOIN   "_SYS_BIC"."prd.global.ecc/CV_KNA1" k 
	  	  ON 	 v."KUNNR" = k."KUNNR"
	  	  WHERE  "POSNR" = '000000'
	  	  AND    "PARVW" ='WE' ; 
	  	  
   VBEP =  SELECT  t1."VBELN"
                  ,t1."POSNR"
                  ,t1."ETENR"
                  ,t1."BMENG"
                  ,SUM(t2."BMENG") AS "ExpectedQtyCumulative"
           FROM   "_SYS_BIC"."prd.global.ecc/CVNS_VBEP" t1 
           INNER JOIN "_SYS_BIC"."prd.global.ecc/CVNS_VBEP" t2 
           ON     t1."VBELN" = t2."VBELN" 
           AND    t1."POSNR" = t2."POSNR" 
           AND    t1."ETENR" >= t2."ETENR"
           WHERE  t1."BMENG" > 0
           AND    t1.VBELN IN (SELECT VBELN FROM :I_TAB_VBELN)
           GROUP BY t1."VBELN",t1."POSNR",t1."ETENR", t1."BMENG"  ;  
           
  H = (SELECT  VBELN,POSNR,EDATU
       FROM    "_SYS_BIC"."prd.global.ecc/CVNS_VBEP"
       WHERE   ETENR = '0001' 
       AND     VBELN IN (SELECT VBELN FROM :I_TAB_VBELN));
       
  
                                  
  F1 = SELECT  Max(a."VBELN") AS "VBELN"
              ,a."VBELV"
              ,a."POSNV"
              ,SUM(CASE WHEN b."VBTYP_N" = 'R' AND b."PLMIN" = '+' THEN IFNULL(b."RFMNG_FLO",0) 
                        WHEN b."VBTYP_N"  = 'h' THEN IFNULL(b."RFMNG_FLO",0) * -1 
                        ELSE 0
                   END)  AS "delvd_qty"
       FROM "_SYS_BIC"."prd.global.ecc/CV_VBFA" a 
       INNER JOIN "_SYS_BIC"."prd.global.ecc/CV_VBFA" b 
       ON    a."VBELN" = b."VBELV" 
       AND   a."POSNN" = b."POSNV"
       WHERE a."STUFE"='00' 
       AND   a.VBELV IN (SELECT VBELN FROM :I_TAB_VBELN)
       AND   a."VBTYP_N" IN ('T','J')                        
       GROUP BY a."VBELV", a."POSNV"; 
       
  F2 = SELECT  a."VBELV"
              ,a."POSNV"
              ,MAX(b."VBELN") AS "LastPGIDoc" 
              ,SUM(CASE WHEN b."VBTYP_N" = 'R' THEN IFNULL(b."RFMNG",0) 
                        WHEN b."VBTYP_N" = 'h' THEN IFNULL(b."RFMNG",0) * -1  
                        ELSE 0
                   END)  AS "PGIQty"
       FROM   "_SYS_BIC"."prd.global.ecc/CV_VBFA" a 
       JOIN   "_SYS_BIC"."prd.global.ecc/CV_VBFA" b 
       ON     a."VBELN" = b."VBELV" AND a."POSNN" = b."POSNV"
       WHERE  a."STUFE" ='00' 
       AND    a.VBELV IN (SELECT VBELN FROM :I_TAB_VBELN)
       AND    a."VBTYP_N" IN ('T','J')
       AND    b."VBELN" LIKE '49%'
       GROUP BY a."VBELV", a."POSNV" ;
                                 
   JEST = SELECT  a."OBJNR"                                                                                    
                ,MAX(CASE WHEN a."STAT" = 'E0008'  THEN 'X' ELSE '' END) AS "FrontLoad"        
                ,MAX(CASE WHEN a."STAT" = 'E0009'  THEN 'X' ELSE '' END) AS "POReceived"    
                ,MAX(CASE WHEN a."STAT" = 'E0001' OR a."STAT" = 'E0004' OR a."STAT" = 'E0005' THEN b."TXT04" 
                          ELSE '' 
                     END) AS "Booking"                                                   
        FROM   "_SYS_BIC"."prd.global.ecc/CV_JEST" a
        LEFT JOIN "_SYS_BIC"."prd.global.ecc/CV_TJ30T" b
        ON     a."STAT" = b."ESTAT"
        WHERE  a."STAT" IN ('E0001', 'E0004', 'E0005', 'E0008', 'E0009')
        AND    a.OBJNR IN (SELECT DISTINCT OBJNR 
                           FROM   "_SYS_BIC"."prd.global.ecc/CVNS_VBAK" AK
                           JOIN   :I_TAB_VBELN I
                           ON     AK.VBELN = I.VBELN )
                           
        AND    a."INACT"<> 'X'
        AND    b."STSMA" = 'Z0000003' 
        AND    b."SPRAS" = 'E'
        GROUP BY a."OBJNR";  
 
 UPSERT "ALEX_CUSTOM"."ZT_M_SALES_ORDER_STATUS_V2" 
       (VBELN,POSNR,ETENR
       ,VBEP_BMENG,VBEP_EDATU,VBEP_LIFSP,VBEP_MBDAT
       ,VBEP_H_EDATU
       ,VBUK_WBSTK,VBUK_LFGSK
       ,VBAK_AUART,VBAK_ERDAT,VBAK_LIFSK,VBAK_FAKSK,VBAK_VKORG,VBAK_KUNNR,VBAK_AUTLF,VBAK_BSTNK,VBAK_WAERK,VBAK_OBJNR,VBAK_VSNMR_V,VBAK_IHREZ,VBAK_ERNAM,VBAK_AUFNR,VBAK_NETWR
       ,VBAP_MATNR,VBAP_WERKS,VBAP_LGORT,VBAP_ABGRU,VBAP_KWMENG,VBAP_ERDAT,VBAP_ERZET,VBAP_FAKSP,VBAP_VSTEL,VBAP_ARKTX,VBAP_NETPR,VBAP_LPRIO
       ,VBAP_CHARG,VBAP_GRKOR,VBAP_PRODH,VBAP_KDMAT,VBAP_ERNAM,VBAP_AEDAT,VBAP_POSEX
       ,MARD_MATNR,MARD_WERKS,MARD_LGORT,MARD_LABST,MARD_KLABS,MARD_KINSM
       ,VBUP_LFSTA
       ,"OpenDlvDoc","OpenDlvQty","QtyShippedTtLine","PGIDate","DlvStatus"
       ,"FrontLoad","POReceived","Booking"
       ,"ShipTo","ShipToName"
       ,"CARRIER","CARRIER_NAME"
       ,"ExpectedQtyCumulative","delvd_qty",F1_VBELN,"PGIQty","LastPGIDoc",ORDER_STATUS)
 
 SELECT COALESCE (EP.VBELN,'0'),COALESCE (EP.POSNR,'0'),COALESCE (EP.ETENR,'0')
       ,EP.BMENG,EP.EDATU,EP.LIFSP,EP.MBDAT
       ,H.EDATU AS EDATU
       ,UK.WBSTK,UK.LFGSK
       ,AK.AUART,AK.ERDAT,AK.LIFSK,AK.FAKSK,AK.VKORG,AK.KUNNR,AK.AUTLF,AK.BSTNK,AK.WAERK,AK.OBJNR,AK.VSNMR_V,AK.IHREZ,AK.ERNAM,AK.AUFNR,AK.NETWR
       ,AP.MATNR,AP.WERKS,AP.LGORT,AP.ABGRU,AP.KWMENG,AP.ERDAT,AP.ERZET,AP.FAKSP,AP.VSTEL,AP.ARKTX,AP.NETPR,AP.LPRIO
       ,AP.CHARG,AP.GRKOR,AP.PRODH,AP.KDMAT,AP.ERNAM,AP.AEDAT,AP.POSEX
       ,NULL AS MATNR,NULL AS WERKS,NULL AS LGORT, NULL AS LABST, NULL AS KLABS, NULL AS KINSM
       ,"UP".LFSTA
       ,NULL AS "OpenDlvDoc",NULL AS "OpenDlvQty",NULL AS "QtyShippedTtLine",NULL AS "PGIDate",NULL AS "DlvStatus"
       ,NULL AS "FrontLoad",NULL AS "POReceived",NULL AS "Booking"
       ,SHIPTO."ShipTo",SHIPTO."ShipToName"
       ,NULL AS "CARRIER",NULL AS "CARRIER_NAME"
       ,T."ExpectedQtyCumulative"
       ,NULL AS "delvd_qty", NULL AS VBELN
       ,NULL AS "PGIQty", NULL AS "LastPGIDoc"
       ,NULL AS ORDER_STATUS
         
FROM        "_SYS_BIC"."prd.global.ecc/CVNS_VBEP" EP
INNER JOIN  "_SYS_BIC"."prd.global.ecc/CV_VBUK" UK
ON EP.VBELN = UK.VBELN
INNER JOIN  "_SYS_BIC"."prd.global.ecc/CVNS_VBAK" AK
ON UK.VBELN = AK.VBELN
INNER JOIN  "_SYS_BIC"."prd.global.ecc/CVNS_VBAP" AP
ON EP.VBELN = AP.VBELN AND EP.POSNR = AP.POSNR

INNER JOIN :H H
ON EP.VBELN = H.VBELN AND EP.POSNR = H.POSNR
INNER JOIN  "_SYS_BIC"."prd.global.ecc/CV_VBUP" "UP"
ON EP.VBELN = "UP".VBELN AND EP.POSNR = "UP".POSNR
INNER JOIN  :VBEP T    
 ON EP."VBELN" = T."VBELN" AND EP."POSNR" = T."POSNR" AND EP."ETENR" = T."ETENR"

INNER JOIN  :SHIPTO SHIPTO                                                 
ON    SHIPTO."VBELN" = EP."VBELN"      
WHERE EP.VBELN IN (SELECT VBELN FROM :I_TAB_VBELN) 
AND   AP.WERKS >= '1900'
AND   AK.AUART NOT IN ('ZVC','ZR07')
;

-----------------------------------------------------

UPDATE MAT_TAB
SET    MARD_MATNR = MARD.MATNR
      ,MARD_WERKS = MARD.WERKS
      ,MARD_LGORT = MARD.LGORT
      ,MARD_LABST = MARD.LABST
      ,MARD_KLABS = MARD.KLABS
      ,MARD_KINSM = MARD.KINSM

FROM  "ALEX_CUSTOM"."ZT_M_SALES_ORDER_STATUS_V2" MAT_TAB
LEFT JOIN (SELECT MATNR,WERKS,LGORT,LABST,KLABS,KINSM
           FROM   "_SYS_BIC"."prd.global.ecc/CV_MARD" ) MARD
ON  MARD.MATNR = MAT_TAB.VBAP_MATNR 
AND MARD.WERKS = MAT_TAB.VBAP_WERKS
AND CASE WHEN IFNULL(MAT_TAB.VBAP_LGORT,'') IN ('','0010') AND MAT_TAB.VBAP_WERKS = '3000' THEN '0030'
         WHEN IFNULL(MAT_TAB.VBAP_LGORT,'') <> '' THEN MAT_TAB.VBAP_LGORT
         ELSE '0010'
    END = MARD.LGORT;
    
UPDATE MAT_TAB
SET    MAT_TAB."delvd_qty"   = F1."delvd_qty"
      ,MAT_TAB.F1_VBELN      = F1.VBELN
      ,MAT_TAB."PGIQty"      = F2."PGIQty"
      ,MAT_TAB."LastPGIDoc"  = F2."LastPGIDoc"
      ,MAT_TAB.ORDER_STATUS  = CASE WHEN IFNULL("VBUK_WBSTK",'') <> 'C' 
		               					 AND  IFNULL("VBAP_ABGRU",'') = '' 
		               					 AND  IFNULL("VBUP_LFSTA",'') <> '' 
		               					 AND  IFNULL("VBAP_KWMENG",0) > IFNULL(F1."delvd_qty",0) 
		               					 AND  IFNULL("VBEP_BMENG",0) > 0 
                       					 AND  CASE WHEN IFNULL("ExpectedQtyCumulative",0) <= IFNULL(F1."delvd_qty",0)  
				                      					THEN 0
                                 				   WHEN (IFNULL("ExpectedQtyCumulative",0) -  IFNULL(F1."delvd_qty",0)) <= IFNULL("VBEP_BMENG",0) 
						              					THEN (IFNULL("ExpectedQtyCumulative",0) -  IFNULL(F1."delvd_qty",0))
                                 				   ELSE IFNULL("VBEP_BMENG",0)
                                              END > 0  
                                         THEN 'Open'
                                    ELSE 'Closed'
                                END
FROM  "ALEX_CUSTOM"."ZT_M_SALES_ORDER_STATUS_V2" MAT_TAB
LEFT JOIN :F1 F1
ON MAT_TAB."VBELN" = F1."VBELV" AND MAT_TAB."POSNR" = F1."POSNV"
LEFT JOIN :F2 F2
ON MAT_TAB."VBELN" = F2."VBELV" AND MAT_TAB."POSNR" = F2."POSNV";

UPDATE MAT_TAB
SET    MAT_TAB."OpenDlvDoc"       = LIKP_LIPS."OpenDlvDoc"
      ,MAT_TAB."OpenDlvQty"       = LIKP_LIPS."OpenDlvQty"
      ,MAT_TAB."QtyShippedTtLine" = LIKP_LIPS."QtyShippedTtLine"
      ,MAT_TAB."PGIDate"          = LIKP_LIPS."PGIDate"
      ,MAT_TAB."DlvStatus"        = LIKP_LIPS."DlvStatus" 
FROM  "ALEX_CUSTOM"."ZT_M_SALES_ORDER_STATUS_V2" MAT_TAB
LEFT JOIN  :LIKP_LIPS LIKP_LIPS
ON LIKP_LIPS.VGBEL = MAT_TAB.VBELN AND LIKP_LIPS.VGPOS = MAT_TAB.POSNR ;


UPDATE MAT_TAB
SET    "FrontLoad"  = JEST."FrontLoad" 
      ,"POReceived" = JEST."POReceived"
      ,"Booking"    = JEST."Booking"
FROM  "ALEX_CUSTOM"."ZT_M_SALES_ORDER_STATUS_V2" MAT_TAB
LEFT JOIN :JEST JEST
ON MAT_TAB.VBAK_OBJNR = JEST.OBJNR;
      

UPDATE MAT_TAB
SET    MAT_TAB."CARRIER"      = CSBG."CARRIER"
      ,MAT_TAB."CARRIER_NAME" = CSBG."CARRIER_NAME"
FROM  "ALEX_CUSTOM"."ZT_M_SALES_ORDER_STATUS_V2" MAT_TAB 
LEFT JOIN  :CSBG CSBG
ON   MAT_TAB."VBELN" = CSBG."ORDER_NO" 
AND  MAT_TAB."POSNR" = CSBG."ORDER_LINE_NO" ;

END;