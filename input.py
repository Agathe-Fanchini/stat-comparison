project_id = int(input('Enter your project id:'))
start_date = input('Enter the start date (AAAA-MM-DD):')
end_date = input('Enter the end date (AAAA-MM-DD):')
device_id = input('Enter your device id (separated by comma):')
page_condition = input('Enter your page condition:')

'''project_id = int(958)
start_date = '2022-09-01'
end_date = '2022-09-30'
device_id = 2
page_condition = "(arrayExists((x1, x2) -> x1 = 2 AND x2 = 'home  page', custom_vars_view.position, custom_vars_view.value) AND NOT(position(query, 'cs-popin-ajout-wishlist') > 0) AND NOT(position(query, 'cs-popin-recherche') > 0) AND NOT(position(query, 'cs-popin-ajout-panier') > 0) AND NOT(position(query, 'cs-popin-menu-mobile') > 0) AND NOT(position(query, 'cs-popin-etre-alerte') > 0) AND NOT(position(query, 'cs-popin-mon-espace-berluti') > 0) AND NOT(position(query, 'cs-popin-panier-mobile') > 0) AND NOT(position(query, 'cs-inscription') > 0) AND NOT(position(query, 'cs-etape2-inscription') > 0) AND NOT(position(query, '\\?cs-popin-panier') > 0) AND NOT(position(query, 'cs-popin-search-result') > 0)) OR (arrayExists((x1, x2) -> x1 = 2 AND x2 = 'home  page', custom_vars_view.position, custom_vars_view.value) AND empty(query))"
"(path = '/fr/mode/fille' AND NOT(position(query, 'popin') > 0)) OR (endsWith(path, '/') AND position(prefix, 'fr.') > 0 AND startsWith(path, '/fr/') AND NOT(position(query, 'cs-popin_ajout_au_panier') > 0) AND NOT(position(query, 'cs-popin_filtres') > 0)) OR (endsWith(path, '/') AND position(prefix, 'fr.') > 0 AND startsWith(path, '/fr/') AND NOT(position(query, 'cs-popin_ajout_au_panier') > 0) AND NOT(position(prefix, 'fr.') > 0)) OR path = '/fr/babyzen.html' OR path = '/fr/puericulture.html'"
'''
dep_var = int(input('Choose your dependent variable (the behaviour you want to explain) -> Bounce=1, Exit=2, Transaction=3 (enter the number):'))
indep_var = int(input('Choose your independent variable (the variable that could explain the previous behaviour) -> Loading Time=1, Scroll=2, View duration=3, Number of pages viewed=4, Activity rate=5 (enter the number):'))

### connection gsheet ###
"""sheet_id=input('sheet_id:')"""
sheet_id = '1xkxiRULqxE234IxXRDADd0Y1LUFLLIaW6aEsJY6QhqI'


