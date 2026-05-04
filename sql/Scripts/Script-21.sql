

select 
                    PAS_FMTG_ID as fmtg_id,
                    PAS_code3 as apaleo_id,
                    PAS_name_short as name_short,
                    PAS_name_long  as name_long,
                    PAS_Protel_ID as protel_id,
                    PAS_GMS_ID as jotform_id,
                    PAS_pms as pms
                    
            from V2D_Property_Attributes vdpa
            where is_active = 1 and PAS_GMS_ID is not null