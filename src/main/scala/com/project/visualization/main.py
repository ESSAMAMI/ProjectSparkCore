import plotly.graph_objects as go
import plotly.figure_factory as ff
import pandas as pd
import plotly.express as px
#
topClients = pd.read_csv('topClients.csv', sep=";", encoding="ISO-8859-1")
print(topClients.head(10))

tableClients = []
for i in range(len(topClients)):
    if i == 0:
        tableClients.append(topClients.columns)
    else:
        tableClients.append(topClients.values[i])
        if i == 100: # TOP 100 Clients
            break

import plotly.express as px

# fig = px.bar(topClients[:100], y='Profits', x='Name', text='Profits')
# fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
# fig.update_layout(uniformtext_minsize=10, uniformtext_mode='hide')
# fig.show()


bestItems = pd.read_csv('bestItems.csv', sep=";", encoding="ISO-8859-1")
print(bestItems.head(10))

tableBestItems = []
for i in range(len(bestItems)):
    if i == 0:
        tableBestItems.append(bestItems.columns)
    else:
        tableBestItems.append(bestItems.values[i])
        if i == 100: # TOP 100 Items
            break

import plotly.express as px
df = px.data.gapminder().query("year == 2007").query("continent == 'Europe'")

fig = px.pie(bestItems, values='Total', names='Category', title='<b>Total / Category</b>')
fig.show()
