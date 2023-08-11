# HD_OB_Store_Tracking
#to track status of ob PO to send to store
#Select * from (
SELECT wms.* except(BOL_NBR, TRAILER_NO, OB_PO_NBR),
Vendor_Name,
sthier.Store, sthier.Store_Name, sthier.Region, sthier.Province, sthier.District_Name, Total_Stock, Instock, Instock_Relevant, ABC, Inv_Date,
Article_Desc, Dept, CLASS_DESC, SUBCLASS_DESC,

c3.* except(PurchaseOrderNumber),
Coalesce(wms.OB_PO_NBR, c3.PurchaseOrderNumber) as OB_PO_NBR,
Coalesce(wms.BOL_NBR, c3.BOL) as BOL_NBR,
Coalesce(wms.TRAILER_NO, c3.C3_TRAILER) as TRAILER_NO,
Coalesce(wms.O_FACILITY_ALIAS_ID, left(c3.C3_Load_Origin,4)) as Origin_Facility,

DateTime(timestamp(AppointmentCreatedTimeStampUTC),
case SOURCE_SFC
  when '7275' then 'America/Toronto'
  when '7279' then 'America/Edmonton'
  else 'America/Toronto'
  end) as AppointmentCreatedTimeStamp, 
if(C3_Appointment is null, LPN_FACILITY_STATUS, 100) as Load_Status,
if(C3_Appointment is null, LPN_FACILITY_STATUS_DESCRIPTION, "Store Appointment Booked") as Load_Status_Desc, 
#Cast(TEMPB='01' as INT64) as Freezable_Flag,
Hazmat,
Delivery_Comp_Ind,
Order_Reason_Cd, REASON_DESC, GR_Qty,
Store_Rounding_UOM, Store_Rounding_Pack_Qty, Dim_Volume, Vol_Unit,
Safe_Divide(INITIAL_QTY,Store_Rounding_Pack_Qty)*Dim_Volume*
CASE Vol_Unit
    WHEN 'CDM' THEN 0.0353147
    WHEN 'M3' THEN 35.315
    ELSE 1 END as Est_Case_Volume,
FSCL_WK_NBR AS APPT_FSCL_WK_NBR,
FSCL_YR_WK_KEY_VAL AS APPT_FSCL_YR_WK_KEY_VAL,
FSCL_WK_BGN_DT AS APPT_FSCL_WK_BGN_DT,
FSCL_WK_END_DT AS APPT_FSCL_WK_END_DT,
FSCL_PRD_NBR AS APPT_FSCL_PRD_NBR,
FSCL_MTH_NM AS APPT_FSCL_MTH_NM,
DAY_OF_YR_NBR AS APPT_DAY_OF_YR_NBR,
DAY_NM AS APPT_DAY_NM,
WEEK_DAY_FLG AS APPT_WEEK_DAY_FLG,
Store_LT,
date_Add(Date(OLPN_SHIPPED_DTTM), INTERVAL 
Store_LT+
  if(
    extract(DAYOFWEEK from date_Add(Date(OLPN_SHIPPED_DTTM), INTERVAL 
    Store_LT day)) IN (1,7),
    2-if(Date(OLPN_SHIPPED_DTTM) between Weekend_Recg_Start and Weekend_Recg_End,Weekend_Recg_Days,0),0)
Day) as Theoretical_Book_Date,
#store booking cutoff time to work in later
#west, stop booking 7pm local time
#east, stop booking 9pm local
Weekend_Recg_Start,
Weekend_Recg_End,
Weekend_Recg_Days,
Current_datetime("America/Toronto") as Process_Day
from (
SELECT
  Coalesce(Vendor_Number, Vendor_Number_Alt) AS Vendor_Number,
  O_FACILITY_ALIAS_ID,
  SOURCE_SFC,
  LPN_STATUS,
  LPN_DETAIL_STATUS,
  #ORIGINAL_TC_SHIPMENT_ID,
  ASN_ID,
  SHIP_VIA as CARRIER,
  BOL_NBR,
  FREIGHT_BILL_NO2,
  TRAILER_NO,
  SEAL_NUMBER,
  #LPN_SHIPPED_FLAG,
  #INBOUND_OUTBOUND_INDICATOR,
  #OLPN_LOADED_DTTM,
  OLPN_SHIPPED_DTTM,
  GATE_OUT,
  #ORD_ACTUAL_SHIPPED_DTTM,
  LPN_FACILITY_STATUS,
  LPN_FACILITY_STATUS_DESCRIPTION,
  DESTINATION_SITE,
  coalesce(ITEM_NAME, ARTICLE) AS ARTICLE,
  #IS_HAZMAT,
  SFC_IB_PO_NBR,
  #DISPO,
  SFC_IB_PO_TYPE,
  OB_PO_TYPE,
  OB_PO_NBR,
  #OLPN_DISPOSITION_TYPE,
  MAX(OLPN_LOADED_DTTM) as OLPN_LOADED_DTTM,
  SUM(OLPN_ESTIMATED_VOLUME) AS OLPN_ESTIMATED_VOLUME,
  SUM(OLPN_ESTIMATED_WEIGHT) AS OLPN_ESTIMATED_WEIGHT,
  SUM(TOTAL_LPN_QTY) AS TOTAL_LPN_QTY,
  SUM(SHIPPED_QTY) AS SHIPPED_QTY,
  SUM(INITIAL_QTY) AS INITIAL_QTY
FROM
  `analytics-ca-scm-thd.GRAEME.STORES_IB_SFC_OB`
where Date(OLPN_CREATED_DTTM) >= Date_Sub(Current_Date(), Interval 28 Day)
and OLPN_CRNT_ROW_IND and OLPN_LATEST_FLG
and OLPND_CRNT_ROW_IND and OLPND_LATEST_FLG
GROUP BY
  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,  11,  12,  13,  14,  15,  16,
  17,  18,  19,  20,  21#,  22,  23,  24,  25#,  26,  27#,  28#,  29
) WMS

/*full outer join (Select * from `analytics-ca-scm-thd.WHS.OUTBOUND_LOG` where Process_day = (Select MAX(PROCESS_DAY) from `analytics-ca-scm-thd.WHS.OUTBOUND_LOG`)) obl
on wms.BOL_NBR = obl.BOL_NUM and wms.TRAILER_NO = obl.TRAILER
#on wms.FREIGHT_BILL_NO2 = obl.FB_NUM and wms.DESTINATION_SITE = obl.Dest
and wms.TRAILER_NO = obl.TRAILER*/

full outer join (SELECT
SFC_Location as C3_Load_Origin,
Trailer as C3_Trailer,
Shipment_ID,
BOL as BOL_Mix,
BOL_Join as BOL,
PurchaseOrderNumber,
Store_Number,
#SubWarehouseColumnName,
Comment,
AppointmentCreatedTimeStampUTC,
DateTime(timestamp(LastActionTimeStampUTC),Timezone) as LastActionTimeStamp,
cast(DateTime(timestamp(Appointment_DatetimeUTC),Timezone) as TIMESTAMP) as C3_Appointment,
Carrier_Name as C3_Carrier_Name,
 FROM `analytics-ca-scm-thd.C3.C3_APPT_ARCHIVE` 
JOIN (SELECT
  BOL,
  BOL_Join
FROM (
  SELECT
    BOL,
    SPLIT(BOL, ",") AS BOL_SPLIT,
  FROM
    `analytics-ca-scm-thd.C3.C3_APPT_ARCHIVE`
  WHERE
    LATEST_FLG),
  UNNEST (BOL_SPLIT) AS BOL_Join)
  using (BOL)
 
 left join `analytics-ca-scm-thd.C3.STORE_TIMEZONES` 
 on Store_Number = Store
 where LATEST_FLG 
  and Appointment_DatetimeUTC >= Date_Sub(Current_Date(), Interval 26 WEEK)
  and upper(coalesce(Discarded,"false")) Not in ('TRUE') 
  and SFC_Location is not null
  and SFC_Location <> ''
#section below added on 2021-11-22 for supplemental current day appointment data
/*UNION ALL 
SELECT 
Replace(Origin,' ','') as C3_Load_Origin,
Trailer as C3_Trailer,
Shipment_ID,
BOL_Summary as BOL_Mix,
BOL_JOIN as BOL,
Purchase_Orders as PurchaseOrderNumber,
Store as Store_Number,
#Null as SubwarehouseColumnName,
Comments as Comment,
Parse_Datetime('%m/%d/%Y %H:%M',Created_Timestamp_UTC) as AppointmentCreatedTimeStampUTC,
Cast(Null as DATETIME) as LastActionTimeStamp,
Cast(Parse_Datetime('%m/%d/%Y %H:%M',Appointment) as Timestamp) as C3_Appointment,
Load_Type as C3_Carrier_Name,
 FROM `analytics-ca-scm-thd.C3.C3_SUPPLEMENTAL` 

JOIN (SELECT
  BOL_Summary,
  BOL_Join
FROM (
  SELECT
    BOL_Summary,
    SPLIT(BOL_Summary, ",") AS BOL_SPLIT,
  FROM
    `analytics-ca-scm-thd.C3.C3_SUPPLEMENTAL`),
  UNNEST (BOL_SPLIT) AS BOL_Join)
  using (BOL_Summary)

left join `analytics-ca-scm-thd.C3.STORE_TIMEZONES` 
 Using(Store)
 where region is not null
 and Parse_Datetime('%m/%d/%Y %H:%M',Created_Local_TZ) >= Current_DAte('America/Toronto')
  
*/ ) c3
ON
wms.BOL_NBR = c3.BOL 

left join `ca-pr-cda-views.PURCHASE_ORDERS.PURCHASE_ORDERS` po
on wms.OB_PO_NBR = po.PO_Nbr and wms.Article = po.Article
left join `analytics-ca-scm-thd.MASTER_DATA.PO_REASON_DESC` rs
on po.Order_Reason_Cd = rs.REASON_CODE

left outer join `analytics-ca-scm-thd.REFERENCE_TABLES.VENDOR_NAMES` ven on wms.VENDOR_NUMBER = ven.VENDOR_NUMBER

left join (
Select rnd.Article, Site, Store_Rounding_UOM, Store_Rounding_Pack_Qty, Dim_Volume, Vol_Unit 
FROM `ca-pr-cda-views.SUPPLY_CHAIN.SRCRND_SC_SOURCE_ROUNDING` rnd
left join `analytics-ca-scm-thd.MASTER_DATA.MARM_SUMMARY` marm
on rnd.Article = marm.Article and rnd.Store_Rounding_UOM = marm.UoM
where date(Process_day) = Date_Sub(Current_Date("America/Toronto"), interval 1 DAY)) rnd
on WMS.ARTICLE = rnd.Article and wms.DESTINATION_SITE = rnd.Site

left join `analytics-ca-scm-thd.GRAEME.STORE_INSTOCK` instck on wms.Article = instck.Article and DESTINATION_SITE = instck.Store
left join `analytics-ca-scm-thd.MASTER_DATA.MATERIAL_DESC_CLASS` matl on wms.Article = matl.MATERIAL
join `analytics-ca-scm-thd.MASTER_DATA.STORE_HIERARCHY` sthier
  #using (Store)
  on coalesce(wms.DESTINATION_SITE, c3.Store_Number) = sthier.Store
left join `analytics-ca-scm-thd.REFERENCE_TABLES.POS_FISCAL_CALENDAR` cal
  on Date(C3_Appointment) = cal.CAL_DT
left join `analytics-ca-scm-thd.GRAEME.STORE_TRANSIT_TIME` trans
  on coalesce(wms.DESTINATION_SITE, c3.Store_Number) = trans.store
where sthier.region is not null

  #and O_FACILITY_ALIAS_ID is null
#where DESTINATION_SITE = '7137' and C3_Appointment	 is null
#where Date(wms.OLPN_CREATED_DTTM) >= Date_Sub(Current_Date(), Interval 21 Day)
#limit 1000
