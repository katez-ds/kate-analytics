select 
case when wbd_fee_promo_discount > 0 then 1
else 0 end wbd_ind,
case when cs_fee_promo_discount > 0 then 1
else 0 end xs_ind,
case when pad_fee_promo_discount > 0 then 1
else 0 end pad_ind,
case when mx_funded_cx_discount > 0 then 1
else 0 end mx_ind,
case when dd_funded_cx_discount > 0 then 1
else 0 end crm_ind,
count(distinct delivery_id) orders
from proddb.static.df_sf_promo_discount_delivery_level
where year(created_at) = 2025
group by 1,2,3,4,5

WBD_IND	XS_IND	PAD_IND	MX_IND	CRM_IND	ORDERS
1	0	0	1	0	22732809
0	1	0	1	1	513750
1	0	0	0	1	9192562
0	0	1	1	0	167
1	1	0	1	1	4006
0	0	0	1	0	271545507
0	1	0	0	1	1169139
0	1	0	0	0	46870111
1	0	1	1	1	4077820
0	0	0	1	1	48581771
0	0	0	0	0	2044342340
0	0	1	1	1	4354977
1	0	1	0	1	921004
1	1	0	0	1	13344
1	0	0	1	1	2786810
1	0	1	0	0	19111677
1	0	1	1	0	191
1	0	0	0	0	128559116
0	0	0	0	1	88218095
1	1	0	1	0	64316
0	0	1	0	0	23848207
0	1	0	1	0	7703000
1	1	0	0	0	339889
0	0	1	0	1	1068835
