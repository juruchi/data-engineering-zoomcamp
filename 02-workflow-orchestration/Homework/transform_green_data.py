if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@transformer
def transform(data, *args, **kwargs):

#   - Add a transformer block and perform the following:
#   - Remove rows where the passenger count is equal to 0 _and_ the trip distance is equal to zero.
#   - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
#   - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.

    data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.columns = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower()
    )

    print(data['vendor_id'].unique())
    return data


@test
def test_output(output, *args) -> None:

#   - Add three assertions:
#     - `vendor_id` is one of the existing values in the column (currently)
#     - `passenger_count` is greater than 0
#     - `trip_distance` is greater than 0
    
    assert 'vendor_id' in output.columns
    assert output['passenger_count'].isin([0]).sum() == 0
    assert output['trip_distance'].isin([0]).sum() == 0


    assert output is not None, 'The output is undefined'
