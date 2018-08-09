-- start query 1 in stream 0 using template query21.tpl and seed 1819994127
select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as timestamp) < cast ('1998-04-08' as timestamp))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as timestamp) >= cast ('1998-04-08' as timestamp))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between date_add(cast ('1998-04-08' as timestamp), interval -30 days)
                    and date_add(cast ('1998-04-08' as timestamp), interval 30 days)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;summary;

-- end query 1 in stream 0 using template query21.tpl
