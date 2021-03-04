--------------------------------------------------------------
--Redshift
--Brand Growth
--------------------------------------------------------------
select
	dcp.product_brand,
	dcp.molecule_name,
	extract(year
from
	foc.order_placed_at) as "year",
	extract(month
from
	foc.order_placed_at) as month_num,
	to_char(foc.order_placed_at, 'Month') AS Month_name,
	sum(mnql.quantity) as Quantity_Sold,
	sum(mnql.unit_mrp * mnql.quantity) as GMV
from
	data_model.f_order_consumer foc
left join data_model.f_order fo on
	fo.order_id = foc.order_id
left join pe_pe2_pe2.medicine_notes mn on
	mn.order_id = fo.order_id
left join pe_pe2_pe2.medicine_notes_product_info mnpi on
	mnpi.medicine_notes_id = mn.id
left join pe_pe2_pe2.medicine_notes_quantity_log mnql on
	mnql.medicine_notes_id = mn.id
left join data_model.d_catalog_product dcp on
	dcp.ucode = mn.ucode
where
	foc.order_status_id in(9, 10)
	AND dcp.product_brand IN ( 'STAMLO', 'STAMLO T', 'STAMLO BETA', 'RECLIDE', 'RECLIMET', 'TELSARTAN', 'TELSARTAN AM', 'TELSARTAN H', 'GLIMY', 'GLIMY MV', 'GLIMY M', 'OMEZ', 'OMEZ DSR', 'ATARAX', 'MINTOP', 'DOXT SL', 'CLAMP', 'XYZAL', 'CETZINE', 'KETOROL', 'RAZO', 'RAZO D', 'IMMULINA', 'AZIWOK', 'CLOHEX', 'ATOCOR', 'ROZAT' )
	and extract(year
from
	foc.order_placed_at) in (2020, 2021)
group by
	1,
	2,
	3,
	4,
	5
order by
	3,
	4;


--------------------------------------------------------------
--Redshift
--Molecule Growth
--------------------------------------------------------------
select
	dcp.molecule_name,
	extract(year
from
	foc.order_placed_at) as "year",
	extract(month
from
	foc.order_placed_at) as month_num,
	to_char(foc.order_placed_at, 'Month') AS Month_name,
	sum(mnql.quantity) as Quantity_Sold,
	sum(mnql.unit_mrp * mnql.quantity) as GMV
from
	data_model.f_order_consumer foc
left join data_model.f_order fo on
	fo.order_id = foc.order_id
left join pe_pe2_pe2.medicine_notes mn on
	mn.order_id = fo.order_id
left join pe_pe2_pe2.medicine_notes_product_info mnpi on
	mnpi.medicine_notes_id = mn.id
left join pe_pe2_pe2.medicine_notes_quantity_log mnql on
	mnql.medicine_notes_id = mn.id
left join data_model.d_catalog_product dcp on
	dcp.ucode = mn.ucode
where
	foc.order_status_id in(9, 10)
	AND dcp.molecule_name IN (
	'AMLODIPINE','TELMISARTAN+AMLODIPINE','AMLODIPINE+ATENOLOL','GLICLAZIDE','GLICLAZIDE+METFORMIN','TELMISARTAN','TELMISARTAN+HYDROCHLOROTHIAZIDE','GLIMEPIRIDE','METFORMIN+GLIMEPIRIDE','VOGLIBOSE+METFORMIN+GLIMEPIRIDE','OMEPRAZOLE','OMEPRAZOLE+DOMPERIDONE','HYDROXYZINE','MINOXIDIL','DOXYCYCLINE+LACTOBACILLUS SPOROGENES','CLAVULANIC ACID+AMOXYCILLIN / AMOXICILLIN','LEVOCETIRIZINE','CETIRIZINE','KETOROLAC','RABEPRAZOLE','RABEPRAZOLE+DOMPERIDONE','PIDOTIMOD','AZITHROMYCIN','CHLORHEXIDINE','ATORVASTATIN','ROSUVASTATIN'
	)
	and extract(year
from
	foc.order_placed_at) in (2020, 2021)
group by
	1,
	2,
	3,
	4
order by
	2,
	3;

--------------------------------------------------------------
--Hive
--Page views
--------------------------------------------------------------
set hive.groupby.orderby.position.alias=true ;

select 
date_format(dt,'MM-YYYY') as MonthYear, event_props_product_id, 
count(profile_objectid) from pe_consumer_ct.p_medicine_details_snapshot
where event_props_product_id in (
--other Product ID not inserted in this code
48910,48914,48915,212023,235038,48911,8579,197843,173904,8619,8578,8580,8576,8577,8581,35717,35715,35719,35716,35714,35718,212578,35713,8497,8207,8315,8209,8508,182216,8314,8452,8480,202428,202411,203869,40963,202297,40961,203884,202390,40962,40966,172588,40964,27556,27579,3016427,27552,3010283,528861,8615,211035,3060642,40144,40143,40145,204807,51971,51970,51969,197321,8446,8234,8232,8235,199094,8233,8475,205760,8236,194143,29526,29525,29533,36390,177095,90814,29428,36396,36391,36394,3038712,36388,175013,29549,29457,36387,200330,41265,35805,234572,35801,188415,8418,233733,174696,35803,8465,35804,35802,173769,202703,188147,173753,173235,170823,189778,8300,191852,169823,8255,169190,8256,170374,8421,8253,169368
)
and dt between '2020-11-01' and '2021-02-28'
group by 1,2

--------------------------------------------------------------

