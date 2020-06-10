### Project: FoodWeb&Me
### Author: Zhao Sun

from neo4j import GraphDatabase
driver = GraphDatabase.driver(uri="bolt://localhost", auth=("neo4j", "password_neo4j"),encrypted=False)


### Initial State Report
with driver.session() as session:
    result = session.run("MATCH (nodes) RETURN count(nodes) ")
print(result.keys(), result.values())

with driver.session() as session:
    result = session.run("MATCH relationships = ()-->() RETURN count(relationships) ")
print(result.keys(),result.values())

with driver.session() as session:
    result = session.run("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 3 ")
print(result.keys(), result.values())


### This should only run once and before importing and creating nodes -  error will be thrown if attempting to create the same index twice
with driver.session() as session:
     # session.run("CREATE INDEX ON :FoodItem(name); ")
     session.run("CREATE INDEX ON :Recipe(name); ")
     session.run("CREATE INDEX ON :Meal(name); ")
     session.run("CREATE INDEX ON :Person(name); ")


### Create the base set consisting of 18 vertices corresponding to energy, nutrients and ecological factors

with driver.session() as session:
    session.run(
        "CREATE (v0:Energy {name:'Calorie',unit:'kcal', tag: 'Energ_Kcal'}) "
        
        "CREATE (v1:Nutrient {name: 'Protein', unit:'g', tag: 'Protein_(g)'}) "
        "CREATE (v2:Nutrient {name: 'Fat', unit:'g', tag: 'Lipid_Tot_(g)'}) "
        "CREATE (v3:Nutrient {name: 'Carbohydrate', unit:'g', tag: 'Carbohydrt_(g)'}) "
        "CREATE (v4:Nutrient {name: 'Fiber', unit:'g', tag: 'Fiber_TD_(g)'}) "
        "CREATE (v5:Nutrient {name: 'Sugar', unit:'g', tag: 'Sugar_Tot_(g)'}) "
        "CREATE (v6:Nutrient {name: 'Cholesterol', unit:'mg', tag: 'Cholestrl_(mg)'}) "     
        "CREATE (v7:Nutrient {name: 'Calcium', unit:'mg', tag: 'Calcium_(mg)'}) "
        "CREATE (v8:Nutrient {name: 'Iron', unit:'mg', tag: 'Iron_(mg)'}) "
        "CREATE (v9:Nutrient {name: 'Sodium', unit:'mg', tag: 'Sodium_(mg)'}) "
        "CREATE (v10:Nutrient {name: 'Folic_Acid', unit:'µg', tag: 'Folic_Acid_(µg)'}) "
        "CREATE (v11:Nutrient {name: 'Vit_A', unit:'IU', tag: 'Vit_A_IU'}) "
        "CREATE (v12:Nutrient {name: 'Vit_B6', unit:'mg', tag: 'Vit_B6_(mg)'}) "
        "CREATE (v13:Nutrient {name: 'Vit_B12', unit:'µg', tag: 'Vit_B12_(µg)'}) "
        "CREATE (v14:Nutrient {name: 'Vit_C', unit:'mg', tag: 'Vit_C_(mg)'}) "
        "CREATE (v15:Nutrient {name: 'Vit_D', unit:'IU', tag: 'Vit_D_IU'}) "

        "CREATE (v16:EcoFactor {name:'GHG',unit:'kg_CO2_eq', tag: 'GHG_kg_CO2_eq'}) "
        "CREATE (v17:EcoFactor {name:'Land',unit:'sqm_year', tag: 'Land_Use_sqm_year'}) "
        # "CREATE (v18:EcoFactor {name:'Water',unit:'kL', tag: 'Freshwater_Withdrawals_kL'}) " # Removed due to lack of data
    )


### Import data from USDA Nutrition Database (source file: SR28 Abbreviated)
### Create 'star-nodes' for 8790 food items. For each node, create relationships with the base set nodes representing composition and set numerical quantities as the edge weights
### We are using 'star-nodes' to represent hyperedges.

with driver.session() as session:
    session.run(
        "LOAD CSV WITH HEADERS from 'file:///ABBREV_SR28_USDA.csv' as row "
        "MERGE (f:FoodItem {name: row.Shrt_Desc}) "
    )

with driver.session() as session:
    session.run(
        "LOAD CSV WITH HEADERS from 'file:///ABBREV_SR28_USDA.csv' as row "
        "WITH row, [key in keys(row) WHERE row[key] IS NOT NULL] as keys "
        "MATCH (v:Energy) WHERE v.tag in keys "
        "MATCH (f:FoodItem) WHERE f.name = row.Shrt_Desc "
        "MERGE (f) -[:CONTAINS_PER_100g {amount: toFloat(row[v.tag])}]->(v)"
    )

with driver.session() as session:
    session.run(
        "LOAD CSV WITH HEADERS from 'file:///ABBREV_SR28_USDA.csv' as row "
        "WITH row, [key in keys(row) WHERE row[key] IS NOT NULL] as keys "
        "MATCH (v:Nutrient) WHERE v.tag in keys "
        "MATCH (f:FoodItem) WHERE f.name = row.Shrt_Desc "
        "MERGE (f)-[:CONTAINS_PER_100g {amount: toFloat(row[v.tag])}]->(v)"
    )

with driver.session() as session:
    session.run(
        "LOAD CSV WITH HEADERS from 'file:///EcoFactors.csv' as row "
        "WITH row, [key in keys(row) WHERE row[key] IS NOT NULL] as keys "
        "MATCH (v:EcoFactor) WHERE v.tag in keys "
        "MATCH (f:FoodItem) WHERE f.name STARTS WITH row.Food "
        "MERGE (f)-[:CONTAINS_PER_100g {amount: toFloat(row[v.tag])}]->(v)"
    )



###########################################################################################################
# Test
# with driver.session() as session:
#     result = session.run("MATCH (nodes:FoodItem) RETURN count(nodes) ")
# print(result.keys(), result.values())
#
# with driver.session() as session:
#     result = session.run("MATCH (n) RETURN n Limit 3 ")
# print(result.keys(), result.values())
#
# with driver.session() as session:
#     result = session.run("MATCH (f:FoodItem)-[r:CONTAINS_PER_100g]->(n:Nutrient) RETURN count(r)")
# print(result.keys(), result.values())

###########################################################################################################


# Download BBC GoodFood database according to https://medium.com/neo4j/whats-cooking-approaches-for-importing-bbc-goodfood-information-into-neo4j-64a481906172
# Note: need to load .jason file several times to process different values; only ingredient list, no amount specified.

with driver.session() as session:
    session.run(
        "CALL apoc.load.json('https://raw.githubusercontent.com/mneedham/bbcgoodfood/master/stream_all.json') YIELD value "
        "WITH value.page.article.id AS id, value.page.title AS title, value.page.article.description AS description, value.page.recipe.cooking_time AS cookingTime, value.page.recipe.prep_time AS preparationTime, value.page.recipe.skill_level AS skillLevel "
        "MERGE (r:Recipe {id: id}) "
        "SET r.cookingTime = cookingTime, r.preparationTime = preparationTime, r.name = title, r.description = description, r.skillLevel = skillLevel; "
    )
    session.run(
        "CALL apoc.load.json('https://raw.githubusercontent.com/mneedham/bbcgoodfood/master/stream_all.json') YIELD value "
        "WITH value.page.article.id AS id, value.page.recipe.ingredients AS ingredients MATCH (r:Recipe {id:id}) "
        "FOREACH (ingredient IN ingredients | MERGE (i:Ingredient {name: ingredient}) MERGE (r)-[:CONTAINS_INGREDIENT]->(i)); "
    )
    session.run(
        "CALL apoc.load.json('https://raw.githubusercontent.com/mneedham/bbcgoodfood/master/stream_all.json') YIELD value "
        "WITH value.page.article.id AS id, value.page.recipe.nutrition_info AS nutrition_info "
        "MATCH (r:Recipe {id:id}) "
        "FOREACH (nutrient IN nutrition_info | MERGE (n:NutritionInfo{name: split(nutrient,' ')[0]})"
        "MERGE (r)-[rel:NUTRITION_INFO]->(n)"
        "SET rel.g = CASE toFloat(split(nutrient,' ')[1]) WHEN toFloat(split(nutrient,' ')[1]) IS NULL THEN split(nutrient,' ')[-1] ELSE split(nutrient,' ')[1] END "
        ")"
    )

###########################################################################################################
# Test
# with driver.session() as session:
#     result = session.run(
#         "MATCH (r: Recipe) RETURN count(r)" #11634
#         "MATCH (i: Ingredient) RETURN count(i)" #3077
#         "MATCH (r: Recipe)-[rel:NUTRITION_INFO]->(n) RETURN count(n)" #75959
#     )
# print(result.keys(), result.values())
###########################################################################################################



### Import Personalized Meals - Toy Example

with driver.session() as session:
    session.run("CREATE CONSTRAINT ON (m:Meal) ASSERT m.name IS UNIQUE ")

# Create nodes for meals
with driver.session() as session:
    session.run(
        "LOAD CSV WITH HEADERS from 'file:///Meals_edit.csv' as row "
        "MERGE (m:Meal {name: row.MealName, type: row.Type}) "
    )
# Create edges linking meals to respective components
with driver.session() as session:
    session.run(
        "LOAD CSV WITH HEADERS from 'file:///Meals_edit.csv' as row "
        "MATCH (f:FoodItem) WHERE f.name = row.FoodItem "
        "MATCH (m:Meal {name: row.MealName}) "
        "MERGE (m)- [:CONTAINS_PER_Serving {amount: toFloat(row.Amount)}]->(f)"
    )
# Calculate calories for meals
with driver.session() as session:
    session.run("MATCH (m:Meal) SET m.kcal = 0 ")
    session.run(
        "MATCH (m:Meal) WITH COLLECT(m) AS meals UNWIND meals as m "
        "MATCH (m) - [r1:CONTAINS_PER_Serving] -> (f:FoodItem)- [r2:CONTAINS_PER_100g] ->(v:Energy) "
        "WITH m,r1, COLLECT([f,r2]) AS foods "
        "FOREACH ( f IN foods | SET m.kcal = m.kcal + r1.amount * (f[1]).amount ) "
    ) ### THIS WORKS NOW!!!

# Calculate environmental factors for meals
with driver.session() as session:
    session.run("MATCH (m:Meal) SET m.GHG = 0 ")
    session.run(
        "MATCH (m:Meal) WITH COLLECT(m) AS meals UNWIND meals as m "
        "MATCH (m) - [r1:CONTAINS_PER_Serving] -> (f:FoodItem)- [r2:CONTAINS_PER_100g] ->(v:EcoFactor {name:'GHG'}) "
        "WITH m,r1, COLLECT([f,r2]) AS foods "
        "FOREACH ( f IN foods | SET m.GHG = m.GHG + r1.amount * (f[1]).amount ) "
    )
with driver.session() as session:
    session.run("MATCH (m:Meal) SET m.Land = 0 ")
    session.run(
        "MATCH (m:Meal) WITH COLLECT(m) AS meals UNWIND meals as m "
        "MATCH (m) - [r1:CONTAINS_PER_Serving] -> (f:FoodItem)- [r2:CONTAINS_PER_100g] ->(v:EcoFactor {name:'Land'}) "
        "WITH m,r1, COLLECT([f,r2]) AS foods "
        "FOREACH ( f IN foods | SET m.Land = m.Land + r1.amount * (f[1]).amount ) "
    )


###########################################################################################################
# Test
# with driver.session() as session:
    # session.run("MATCH (m:Meal) detach delete (m)")
    # result = session.run("MATCH (m:Meal) return count(m)")
    # result = session.run("MATCH (m:Meal)-[r]-(f) return count(r)")
    # result = session.run("MATCH (m:Meal) return m.kcal limit 3")
#     result = session.run("MATCH (m:Meal) return m.name, m.type, m.kcal, m.GHG limit 10")
# print(result.keys(), result.values())
###########################################################################################################


### Create Users for Demo
with driver.session() as session:
    session.run("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE ")
    session.run("Create (p:Person {name: 'Viola', daily_calorie_intake_target:1800, diet_type: 'Vege', special_notes:'' })")
    session.run("Create (p:Person {name: 'Harold', daily_calorie_intake_target:2500, diet_type: 'No_Sweet', special_notes:'trying to reduce sugar intake'})")
    session.run("Create (p:Person {name: 'Sophie', daily_calorie_intake_target:2000, diet_type: 'Sweet', special_notes:'sweet tooth and junk food' })")
    session.run("Create (p:Person {name: 'Mike', daily_calorie_intake_target:3000, diet_type: 'Meat', special_notes:'meat-lover and atheletic' })")

# Create daily-menu for each user
with driver.session() as session:
    session.run(
        "WITH ['Day_1','Day_2','Day_3','Day_4','Day_5','Day_6','Day_7','Day_8'] as days UNWIND days as d "
        "MATCH (p:Person), (m:Meal) WHERE p.diet_type = m.type AND m.name ENDS WITH right(d,1) "
        "MERGE (p)-[r:HAD {time_stamp:d}]->(m)"
    )

###########################################################################################################
# Test
# with driver.session() as session:
    # result = session.run("MATCH (p:Person) DETACH DELETE (p) ")
    # result = session.run("MATCH (p:Person) RETURN count(p) ")
    # result = session.run("MATCH r = (p:Person)--() RETURN count(r) ")
    # session.run("MATCH (p:Person)-[r]-(m:Meal) DELETE r")
#     result = session.run("MATCH (p:Person)-[r]-(m:Meal) RETURN p.name,r.time_stamp,m.name limit 20")
# print(result.keys(), result.values())
###########################################################################################################


# Aggregate GHG information for Viola Vegetarian
with driver.session() as session:
    # Create an Aggregator node with specification
    session.run("CREATE (a:Aggregator {members: ['Viola'], time_period:['Day_1','Day_2'], kcal:0, GHG: 0}) ")
    # Attach the Aggregator node to its specified constituents
    session.run("MATCH (a:Aggregator {members: ['Viola'], time_period:['Day_1','Day_2']}) "
                "UNWIND a.members AS name "
                "MATCH (p:Person {name: name}) MERGE (p)-[r:MEMBER_OF]->(a)")
    # Calculate the total calories as a property of the Aggregator node
    session.run("MATCH (a:Aggregator {members: ['Viola'], time_period:['Day_1','Day_2']}) "
                "UNWIND a.members AS name UNWIND a.time_period AS time_stamp "
                "MATCH (p:Person {name: name})-[r:HAD {time_stamp: time_stamp}]-(m:Meal) WITH a, COLLECT(m) AS meals UNWIND meals as m "
                "MATCH (m)-[r1:CONTAINS_PER_Serving]->(f:FoodItem)-[r2:CONTAINS_PER_100g]->(v:Energy) WITH a,r1, COLLECT([f,r2]) AS foods "
                "FOREACH ( f IN foods | SET a.kcal = a.kcal + r1.amount * (f[1]).amount ) "
    )
    # Calculate the GHG as a property of the Aggregator node
    session.run("MATCH (a:Aggregator {members: ['Viola'], time_period:['Day_1','Day_2']}) "
                "UNWIND a.members AS name UNWIND a.time_period AS time_stamp "
                "MATCH (p:Person {name: name})-[r:HAD {time_stamp: time_stamp}]-(m:Meal) WITH a, COLLECT(m) AS meals UNWIND meals as m "
                "MATCH (m)-[r1:CONTAINS_PER_Serving]->(f:FoodItem)-[r2:CONTAINS_PER_100g]->(v:EcoFactor {name: 'GHG'}) WITH a,r1, COLLECT([f,r2]) AS foods "
                "FOREACH ( f IN foods | SET a.GHG = a.GHG + r1.amount * (f[1]).amount ) "
    )



# Output warning message of breaching daily intake limit - Sophie Sugar

with driver.session() as session:
# Set a limit for daily sugar target: 10% of total calorie intake (Source: UN Guidelines); 1g sugar = 4kcal
    session.run("MATCH (p:Person {name: 'Sophie'}) SET p.sugar_target = p.daily_calorie_intake_target * 0.1/4 "
                )
# Create an Aggregator node to contain the summarized information
    session.run("CREATE (a:Aggregator {members: ['Sophie'], time_period:['Day_5','Day_6'], kcal:0, Sugar: 0}) "
                )
    session.run("MATCH (a:Aggregator {members: ['Sophie'], time_period:['Day_5','Day_6']}) "
                "UNWIND a.members AS name MATCH (p:Person {name: name}) "
                "MERGE (p)-[r:MEMBER_OF]->(a) "
                )
# Calculate the accumulative sugar intake by traversing the lower level nodes
    session.run("MATCH (a:Aggregator {members: ['Sophie'], time_period:['Day_5','Day_6']}) "
                "UNWIND a.members AS name UNWIND a.time_period AS time_stamp "
                "MATCH (p:Person {name: name})-[r:HAD {time_stamp: time_stamp}]-(m:Meal) "
                "WITH a, COLLECT(m) AS meals UNWIND meals as m "
                "MATCH (m)-[r1:CONTAINS_PER_Serving]->(f:FoodItem)-[r2:CONTAINS_PER_100g]->(v:Nutrient {name: 'Sugar'}) "
                "WITH a,r1, COLLECT([f,r2]) AS foods "
                "FOREACH ( f IN foods | SET a.Sugar = a.Sugar + r1.amount * (f[1]).amount ) "
                )
# Personalize warning message
    session.run("MATCH (p:Person {name: 'Sophie'})--(a:Aggregator {members: ['Sophie']}) "
                "WHERE a.Sugar > size(a.time_period) * p.sugar_target "
                "WITH a, round(a.Sugar - size(a.time_period) * p.sugar_target) AS overflow "
                "SET a.Warning = 'Sophie, you have consumed '+ toString(overflow) + "
                "'g more sugar than recommended in this period. Please consider reduction and stay healthy!'"
                )

# Calculate the accumulative calorie intake by traversing the lower level nodes
    session.run("MATCH (a:Aggregator {members: ['Sophie'], time_period:['Day_5','Day_6']}) "
                "UNWIND a.members AS name UNWIND a.time_period AS time_stamp "
                "MATCH (p:Person {name: name})-[r:HAD {time_stamp: time_stamp}]-(m:Meal) "
                "WITH a, COLLECT(m) AS meals UNWIND meals as m "
                "MATCH (m)-[r1:CONTAINS_PER_Serving]->(f:FoodItem)-[r2:CONTAINS_PER_100g]->(v:Energy) "
                "WITH a,r1, COLLECT([f,r2]) AS foods "
                "FOREACH ( f IN foods | SET a.kcal = a.kcal + r1.amount * (f[1]).amount ) "
                )

# Output warning message
with driver.session() as session:
    result = session.run("MATCH (p:Person {name: 'Sophie'})--(a:Aggregator {members: ['Sophie']}) RETURN p.sugar_target, a.Sugar, a.Warning ")
print(result.keys(), result.values())

###########################################################################################################
# Test
# with driver.session() as session:
#     # session.run("MATCH (a:Aggregator) DETACH DELETE (a) ")
#     result = session.run("MATCH (a:Aggregator) RETURN (a) ")
#     # result = session.run("MATCH r = (p:Person)--() RETURN count(r) ")
#     # session.run("MATCH (p:Person)-[r]-(m:Meal) DELETE r")
#     # result = session.run("MATCH (p:Person)-[r]-(m:Meal) RETURN p.name,r.time_stamp,m.name limit 20")
# print(result.keys(), result.values())

###########################################################################################################


# Find out recipes with only 4 ingredients
with driver.session() as session:
    session.run("MATCH (n:Recipe)-[r:CONTAINS_INGREDIENT]->() RETURN n.name, COUNT(r)") #11634
    session.run("MATCH (n:Recipe)-[r:CONTAINS_INGREDIENT]->() WITH n, COUNT(r) as num SET n.numIngred = num ")
    session.run("MATCH (n:Recipe) WHERE n.numIngred = 4 RETURN count(n) ") #4: 598, 3: 273, 2: 75

# Assume the user only have limited ingredients at hand, find recipes containing those ingredients and also suggesting additional ingredients needed
with driver.session() as session:
    result = session.run("UNWIND ['egg','onion','tomato','bacon'] AS inventory "
                "MATCH (n:Recipe)-[r:CONTAINS_INGREDIENT]->(i:Ingredient) WHERE i.name = inventory "
                "WITH n, COUNT(r) as count WHERE count = 3 AND n.numIngred < 8 "
                "MATCH z = (n)--(i:Ingredient) RETURN z "
                )
print(result.keys(), result.values())

# Cypher to display schema
"call db.schema.visualization"


################################# END ###################################
