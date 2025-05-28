with all_checking as (
    select
        'patrick' as user
        , column0 as transaction_date
        , column1 as transaction_description
        , column2 as transaction_type
        , column3 as string_amount
    from financedb.source_data.patrick_checking

    -- union all

    -- select
    --     'michaela' as user
    --     , column0 as transaction_date
    --     , column1 as transaction_description
    --     , column2 as transaction_type
    --     , column3 as string_amount
    -- from financedb.source_data.michaela_checking

    union all

    select
        'joint' as user
        , column0 as transaction_date
        , column1 as transaction_description
        , column2 as transaction_type
        , column3 as string_amount
    from financedb.source_data.joint_checking
)

select
    user
    , transaction_date
    , transaction_description
    , transaction_type
    , cast(regexp_replace(string_amount, '[+-]$', '') as float) *
        case when string_amount like '%-' then -1 else 1 end
     as amount
from all_checking
