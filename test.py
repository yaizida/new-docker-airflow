import pandas as pd
from suds.client import Client

KEY = '1vSu3k1DvCE='
client = Client('https://sales-ws.farfetch.com/pub/apistock.asmx?wsdl',
                timeout=30)

response = client.service.GetAllItemsWithStock(KEY)
data = response.GetAllItemsWithStockResult.diffgram[0].DocumentElement[0].Table # noqa
header = [x[0] for x in data[0]]
body = [[str(j[1][0] if isinstance(j[1], list)
         else str(j[1])) for j in row]for row in data]
df = pd.DataFrame(body, columns=header)
print(df.info())
