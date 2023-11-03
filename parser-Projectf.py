from ordered_set import OrderedSet
from collections import OrderedDict
from collections import defaultdict
import csv

import sys

def parse_dataset(file_path):
    """
    Parse the input dataset (CSV file) and store it in a Python dictionary.

    Args:
        file_path (str): The path to the CSV file containing the dataset.

    Returns:
        dict: A dictionary where keys are column names and values are lists of data.
    """
    dataset = {}
    
    try:
        with open(file_path, 'r', newline='') as file:
            csv_reader = csv.DictReader(file)
            
            # Get the column names from the first row of the CSV
            columns = csv_reader.fieldnames
            
            for column in columns:
                dataset[column] = []
            
            for row in csv_reader:
                for column, value in row.items():
                    dataset[column].append(value)
    
    except FileNotFoundError:
        print("Error: File not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return dataset

def parse_functional_dependencies(file_path):
    """
    Parse functional dependencies from a text file.
     Args:
        file_path (str): The path to the text file containing functional dependencies.
    Returns:
        list: A list of functional dependencies.
    """
    dependencies = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    dependencies.append(line)
    except FileNotFoundError:
        print("Error: Functional dependencies file not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return dependencies

def parse_mvd_dependencies(file_path):
    with open(file_path, 'r') as file:
        lines = [line.strip() for line in file]
    
    mvd_dependencies = {}
    for mvd in lines:
        determinant, dependent = mvd.split(" ->> ")
        determinant_str = str(determinant.split(", "))
        mvd_dependencies.setdefault(determinant_str, []).append(dependent)

    return mvd_dependencies
def get_composite_keys():
    """
    Prompt the user for composite/composite keys.
    Returns:
        list: List of composite/composite keys entered by the user.
    """
    keys = input("Enter composite/composite keys (comma-separated): ").strip().split(',')
    return [key.strip() for key in keys]


def closure(attributes, functional_dependencies):
    closure_attributes = set(attributes)
    changed = True
    
    while changed:
        changed = False
        for fd in functional_dependencies:
            lhs, rhs = fd.split('->')
            if set(lhs).issubset(closure_attributes) and rhs not in closure_attributes:
                closure_attributes.add(rhs)
                changed = True
                
    return closure_attributes
def is_superkey(lhs, keys):
    return set(lhs).issuperset(set(keys))
def has_transitive_dependency(candidate_keys, functional_dependencies):
    # First, find all prime and non-prime attributes
    prime_attributes = set()
    for key in candidate_keys:
        prime_attributes.update(key)

    non_prime_attributes = set(parsed_data.keys()) - prime_attributes

    # Now, build a mapping of direct dependencies for each attribute
    attribute_dependencies = {attr: set() for attr in non_prime_attributes}
    for fd in functional_dependencies:
        lhs, rhs = fd.split('->')
        lhs = set(lhs.strip().split(','))
        rhs = set(rhs.strip().split(','))

        # If the LHS is not a superkey and RHS has non-prime attributes, add the dependency
        if not is_superkey(lhs, candidate_keys) and rhs.issubset(non_prime_attributes):
            for attr in lhs:
                attribute_dependencies[attr].update(rhs)

    # Check for transitive dependencies
    for attr in attribute_dependencies:
        for dependent in attribute_dependencies[attr]:
            if dependent in attribute_dependencies and attribute_dependencies[dependent]:
                # Found a transitive dependency: attr -> dependent -> something else
                return True
    return False
def get_normal_form_choice(parsed_data, functional_dependencies, choice,mvd):
    """
    Get the user's choice of the highest normal form to reach.

    Returns:
        str: The user's choice (e.g., "1NF", "2NF", "3NF").
    """
    #return choice
    # Assuming functional dependencies are given in the form of strings like "A, B -> C"
    # This means A and B together determine C.
    if choice=="1NF":
        if check_1nf(parsed_data):
            print("Data is in 1NF")     
        else:
            print("dataset not in 1NF")
            generate_1nf_queries(parsed_data)

    elif choice== "2NF":
        # To convert to 2NF, we need to check if the dataset is already in 1NF
        # and there are no partial dependencies.

        is_1nf = True

        # Check if all columns have atomic values (1NF check)
        for column, values in parsed_data.items():
            for value in values:
                if ',' in value:  # Assuming atomic values don't have commas
                    is_1nf = False
                    break

        if is_1nf:
            # Check for partial dependencies
            for fd in functional_dependencies:
                lhs, rhs = fd.split('->')
                lhs = lhs.strip().split(',')
                rhs = rhs.strip()
                # If lhs is not a superkey, it indicates a partial dependency
                if not set(lhs).issuperset(set(parsed_data.keys())):
                    is_1nf = False
                    break

        if is_1nf:
            return "dataset in 2NF"
        else:
            decomposed_tables=decomposition_2nf(parsed_data, functional_dependencies, composite_keys)
            
            return decomposed_tables
    elif choice=="3NF":
        print('checking 3NF conditions')
        is_1nf = True

        # Check if all columns have atomic values (1NF check)
        for column, values in parsed_data.items():
            for value in values:
                if ',' in value:  # Assuming atomic values don't have commas
                    is_1nf = False
                    break

        is_2nf = True
        if is_1nf:
            # Check for partial dependencies
            for fd in functional_dependencies:
                lhs, rhs = fd.split('->')
                lhs = lhs.strip().split(',')
                rhs = rhs.strip()
                # If lhs is not a superkey, it indicates a partial dependency
                if not set(lhs).issuperset(set(parsed_data.keys())):
                    is_2nf = False
                    break
            output_query_path = "query.txt"
        return generate_3NF_queries(parsed_data, functional_dependencies,output_query_path)
    # Add more cases for higher normal forms if needed.
    #return chosen_normal_form
    elif choice=="BCNF":
        input_relation=decompose_to_3nf(parsed_data, functional_dependencies, composite_keys)
        #input_relations = [
    #{'relation_name': 'students_table', 'attributes': ['StudentID', 'FirstName', 'LastName'], 'data': [
     #   {"StudentID": 101, "FirstName": "John", "LastName": "Doe"},
      # {"StudentID": 103, "FirstName": "Arindam", "LastName": "Khanda"},
       # {"StudentID": 104, "FirstName": "Jose", "LastName": "Franklin"},
        #{"StudentID": 105, "FirstName": "Ada", "LastName": "Lovelace"}
    #]},

    #{'relation_name': 'courses_table', 'attributes': ['Course', 'CourseStart', 'CourseEnd', 'Professor'], 'data': [
     #   {"Course": "Math101", "CourseStart": "1/1/2023", "CourseEnd": "5/30/2023", "Professor": "Dr.Smith"},
      #  {"Course": "CS101", "CourseStart": "2/1/2023", "CourseEnd": "6/15/2023", "Professor": "Dr.Jones"},
       # {"Course": "Bio101", "CourseStart": "3/1/2023", "CourseEnd": "7/20/2023", "Professor": "Dr.Watson"}
    #]},

    #{'relation_name': 'enrollments_table', 'attributes': ['StudentID', 'Course', 'classRoom'], 'data': [
     #   {"StudentID": 101, "Course": "Math101", "classRoom": "M1"},
      #  {"StudentID": 101, "Course": "CS101", "classRoom": "C1"},
       # {"StudentID": 102, "Course": "Math101", "classRoom": "M1"},
        #{"StudentID": 102, "Course": "CS101", "classRoom": "C2"},
        #{"StudentID": 103, "Course": "CS101", "classRoom": "C1"},
        #{"StudentID": 104, "Course": "Bio101", "classRoom": "B1"},
        #{"StudentID": 105, "Course": "CS101", "classRoom": "C1"}
    #]}]
        
        # Perform decomposition
        decomposed_tables = decompose_to_bcnf(input_relation, functional_dependencies)

        # Output the decomposed relations
        for idx, decomposed_table in enumerate(decomposed_tables):
            print(f"Decomposed relation {idx + 1}: {decomposed_table}")
            if is_bcnf(decomposed_table, functional_dependencies): 
                print("Decomposed relation is in BCNF.")
            else:
                print("Decomposed relation is not in BCNF.")
        return decomposed_tables
    elif choice=="4NF":
     #write something here
     decomposed_tables=decompose_to_4NF(parsed_data, mvd)
     
    elif choice=="5NF":
        return 1
        #here too    

def determine_data_type(values): #for query generator function
    # Assuming values is a list of column values
    for value in values:
        try:
            int(value)  # Attempt to convert to integer
            return "INT"
        except ValueError:
            try:
                float(value)  # Attempt to convert to float
                return "FLOAT"
            except ValueError:
                pass

    return "VARCHAR(255)"  # Default to VARCHAR if neither int nor float
def determine_data_type2(value):
    # Determine the data type based on the value
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    else:
        return f"VARCHAR({len(str(value))})"
def check_1nf(dataset):
    is_1nf = True

    # Check if all columns have atomic values (1NF check)
    for column, values in parsed_data.items():
        for value in values:
            if ',' in value:  # Assuming atomic values don't have commas
                is_1nf = False
                break
    return is_1nf

def generate_1nf_queries(dataset):
    queries = []

    # Create a table for each column
    for column, values in dataset.items():
        data_type = determine_data_type(values)
        query = f"CREATE TABLE {column} (\n"
        query += f"{column} {data_type} PRIMARY KEY"
        query += ");"
        queries.append(query)
    with open(output_query_path, 'a') as query_file:
        query_file.write("\n1NF Queries:\n")
        query_file.write('\n'.join(queries))
    return queries

def decomposition_2nf(dataset, functional_dependencies, composite_keys):
    decomposed_tables = []
    # Create a dictionary to store the attributes for each table
    table_attributes = {}
    list_lhs=[]
    list_rhs=[]
    for fd in functional_dependencies:
        lhs, rhs = fd.split('->')
        lhs = lhs.strip().split(',')
        rhs = rhs.strip().replace(" ", "").split(',')
        list_lhs+=lhs 
        list_rhs.append(rhs)
    list(OrderedSet(list_lhs))
    #print(list_lhs)
    #print(list_rhs)
    for id in list_lhs:
        if id not in composite_keys:
            for list1 in list_rhs:
                if id in list1:
                    k=list_lhs.index(id)
                    list1+=list_rhs[k]
                    list_lhs.remove(id)
                    list_rhs.pop(k)
    #print(list_lhs)
    #print(list_rhs)
    if(len(list_lhs)==len(list_rhs)):
        for i in range(len(list_lhs)):
            result=[]
            key1=''.join(str(e) for e in list_lhs[i])
            result.append(key1)
            for item in list_rhs[i]:
                result.append(item)
            print(result)
            table_name = input(f"Enter a new table name for {', '.join(str(e) for e in result)}: ")
            if table_name not in table_attributes:
                table_attributes[table_name] = OrderedSet()
            table_attributes[table_name].update(OrderedSet(result))

    for table_name, attributes in table_attributes.items():
        attributes = list(attributes)
        # Create the decomposed tables with attribute values from the original table
        decomposed_table = {}
        for attr in attributes:
            decomposed_table[attr] = dataset[attr]
        decomposed_tables.append({table_name: decomposed_table})
    print("decomposed table is: \t")
    print(decomposed_tables)
    generate_query(table_attributes,dataset) #generate queries for decomposed tables
    return decomposed_tables

def decompose_to_3nf(dataset, functional_dependencies, candidate_keys):
    queries = []
    decomposed_tables = []
    # Create a dictionary to store the attributes for each table
    table_attributes = OrderedDict()

    # Separate each functional dependency into left-hand side (LHS) and right-hand side (RHS)
    fd_lhs = []
    fd_rhs = []
    for fd in functional_dependencies:
        lhs, rhs = fd.split('->')
        lhs = set(lhs.strip().split(','))
        rhs = set(rhs.strip().split(','))
        fd_lhs.append(lhs)
        fd_rhs.append(rhs)

    prime_attributes = set()
    for key in candidate_keys:
        prime_attributes.update(key)

    non_prime_attributes = set(parsed_data.keys()) - prime_attributes
    # Remove transitive dependencies
    for i, (lhs, rhs) in enumerate(zip(fd_lhs, fd_rhs)):
        if lhs.issubset(non_prime_attributes) and rhs.issubset(non_prime_attributes):
            fd_lhs.pop(i)
            fd_rhs.pop(i)
 
    # Now create tables for the 3NF decomposition
    for lhs, rhs in zip(fd_lhs, fd_rhs):
        # Create a new table with the LHS as the key and RHS as the attributes
        key = ''.join(sorted(lhs))
        attributes = lhs.union(rhs)
        table_name = input(f"Enter a new table name for {', '.join(attributes)}: ")
        if table_name not in table_attributes:
            table_attributes[table_name] = OrderedDict.fromkeys(attributes)
    
    # Assign attributes to the tables
    for table_name, attributes in table_attributes.items():
        decomposed_table = {}
        for attr in attributes:
            decomposed_table[attr] = dataset.get(attr.strip(), [])
        decomposed_tables.append({table_name: decomposed_table})
    
    print("Decomposed table is: \t")
    print(decomposed_tables)
    # Generate queries for decomposed tables
    return generate_query(table_attributes, dataset)


def generate_3NF_queries(dataset, functional_dependencies,output_query_path):
    queries = []
    decomposed_tables = {}
    table_attributes = {}
    
    # Step 1: Split the functional dependencies
    fds = {}  # {lhs: [list of rhs]}
    for fd in functional_dependencies:
        lhs, rhs = fd.split('->')
        lhs = lhs.strip()
        rhs = rhs.strip().replace(" ", "").split(',')
        if lhs in fds:
            fds[lhs].extend(rhs)
        else:
            fds[lhs] = rhs
    
    # Step 2: Decompose based on non-transitive dependencies
    for lhs, rhs in fds.items():
        if lhs not in table_attributes.keys():
            table_attributes[lhs] = [lhs] + rhs
        else:
            table_attributes[lhs].extend(rhs)

    # Step 3: Generate SQL queries based on decomposition
    for table_name, attributes in table_attributes.items():
        data_types = [determine_data_type(dataset[col.strip()]) for col in attributes]
        new_table = f"CREATE TABLE {table_name} (\n"
        new_table += ',\n'.join(f'{col.strip()} {data_type}' for col, data_type in zip(attributes, data_types))
        new_table += f",\nPRIMARY KEY ({attributes[0].strip()})"
        new_table += ');\n'
        queries.append(new_table)
        decomposed_tables[table_name] = attributes
    
    with open(output_query_path, 'a') as query_file:
        query_file.write("\n3NF Queries:\n")
        query_file.write('\n'.join(queries))

    print(f"Data has been written to {output_query_path}")
    print("Decomposed tables are:")
    for table, cols in decomposed_tables.items():
        print(f"{table}: {', '.join(cols)}")

    return decomposed_tables

def is_bcnf(relation, functional_dependencies):
    for fd in functional_dependencies:
        lhs, rhs = fd.split('->')
        lhs = lhs.strip().split(',')
        rhs = rhs.strip()
        
        if set(lhs).issubset(set(relation.keys())) and rhs not in relation.keys():
            return False
    return True

def decompose_to_bcnf(input_relation, functional_dependencies):
    decomposed_relations = []

    for relation_info in input_relation:
        relation_name = relation_info['relation_name']
        attributes = relation_info['attributes']
        data = relation_info['data']
        non_trivial_fds = [fd for fd in functional_dependencies if fd.split('->')[0] != fd.split('->')[1]]

        closure_attributes = closure(attributes, non_trivial_fds)
        if len(closure_attributes) == len(attributes):
            decomposed_relations.append({'relation_name': relation_name, 'attributes': attributes, 'data': data})
        else:
            new_relations = []
            for entry in data:
                key = tuple(entry[attr] for attr in attributes)
                new_relations.append({attr: entry[attr] for attr in closure_attributes})
              
            decomposed_relations.append({'relation_name': relation_name, 'attributes': list(closure_attributes), 'data': new_relations})
            # Generate and write SQL queries to the output file
        output_query_path = "query.txt"
        generate_bcnf_query(decomposed_relations, output_query_path)

    return decomposed_relations
    
#def decompose_to_4NF(relation, mvd, ck):
    # Implement 4NF decomposition algorithm here
    
    # Initialize a list to store the decomposed relations
    #decomposed_relations = [relation]

    # Iterate over each MVD
    #for X, Y in mvd:
        # Check if X is a superkey (candidate key) for the relation
       # if set(X).issuperset(set(ck)):
            # Create a new relation with attributes in Y
           # new_relation = list(set(Y))
            # Add the new relation to the list of decomposed relations
           # decomposed_relations.append(new_relation)
            # Remove attributes in Y from the original relation
            #relation = [attr for attr in relation if attr not in Y]
            #return decomposed_relations
def decompose_to_4NF(relation, mvd_dependencies):
    decomposed_tables = [relation]
    
    for mvd in mvd_dependencies:
        X, Y = mvd.split(" ->> ")
        X = set(X.split(', '))
        Y = set(Y.split(', '))
        
        # Create two new tables
        table1 = {attr: [] for attr in X | Y}
        table2 = {attr: [] for attr in X | (set(relation.keys()) - Y)}
        
        # Distribute data into new tables based on the MVD
        for tuple_data in relation:
            if all(tuple_data[attr] in table1[attr] for attr in X):
                for attr in X:
                    table1[attr].append(tuple_data[attr])
                for attr in Y:
                    table1[attr].append(tuple_data[attr])
            else:
                for attr in X:
                    table2[attr].append(tuple_data[attr])
                for attr in set(relation.keys()) - Y:
                    table2[attr].append(tuple_data[attr])
        
        # Remove attributes that are now in table1
        for attr in X | Y:
            del relation[attr]
        
        # Remove attributes that are now in table2
        for attr in X | (set(relation.keys()) - Y):
            del relation[attr]
        
        decomposed_tables.append(table1)
        decomposed_tables.append(table2)
    
    return decomposed_tables

    
def generate_bcnf_query(decomposed_tables, output_query_path):
    queries = []
    for idx, table_info in enumerate(decomposed_tables):
        table_name = table_info['relation_name']
        attributes = table_info['attributes']
        data = table_info['data']

        # Skip tables with no data
        if not data:
            continue

        # Sample a value to determine data type
        sample_values = {attr: data[0][attr] for attr in attributes}

        data_types = [determine_data_type2(sample_values[attr]) for attr in attributes]

        create_table_query = f"CREATE TABLE {table_name} (\n"
        create_table_query += ',\n'.join([f'{attr} {data_type}' for attr, data_type in zip(attributes, data_types)])
        create_table_query += f",\nPRIMARY KEY ({attributes[0]})"
        create_table_query += ');\n'

        queries.append(create_table_query)
    # Write queries to a file

    with open(output_query_path, 'a') as query_file:
        query_file.write("\nBCNF Queries:\n")
        query_file.write('\n'.join(queries))

    print(f"Data has been written to {output_query_path}")


def generate_query(table_attributes,dataset):  #function to generate queries for decomposed tables
    queries = []
    # Generate CREATE TABLE queries
    for table_name, attributes in table_attributes.items():
        attributes = list(attributes)
        #print(attributes)
        # Determine data types for attributes
        data_types = [determine_data_type(dataset[col.strip()]) for col in attributes]

        # Create the new table
        new_table = f"CREATE TABLE {table_name} (\n"
        new_table += ',\n'.join(f'{col.strip()} {data_type}' for col, data_type in zip(attributes, data_types))
        new_table += f",\nPRIMARY KEY ({attributes[0].strip()})"
        new_table += ');\n'
        queries.append(new_table)
    output_query_path = "query.txt"
    write_to_query_file(queries,output_query_path)
    print(f"Data has been written to {output_query_path}")

def find_highest_normal_form(dataset, functional_dependencies):
    # Implement the logic to find the highest normal form here.
    # This part depends on your specific project requirements.

    # For demonstration, we'll just return the chosen normal form.
    return 0

def write_to_text_file(data, output_file):
    """
    Write the parsed data to a text file.Args:
        data (dict): Parsed data to be written.
        output_file (str): The path to the output text file.
    """
    with open(output_file, 'w') as file:
        for column, values in data.items():
            file.write(f"{column}: {', '.join(values)}\n")

def write_to_query_file(queries,output_file):
    with open(output_file, 'a') as file:
        for query in queries:
            file.write(f"{query}\n")



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python your_script.py input_dataset.csv functional_dependencies.txt")
    else:
        input_file_path = sys.argv[1]
        parsed_data = parse_dataset(input_file_path)

        functional_dependencies_file = sys.argv[2]
        functional_dependencies = parse_functional_dependencies(functional_dependencies_file)
        mvd_file_path = sys.argv[3]  
        

        # Print the parsed data for demonstration
        for column, values in parsed_data.items():
            print(f"{column}: {', '.join(values)}")
        composite_keys = []
        composite_keys = get_composite_keys()

        choice = input("Enter the highest normal form to reach (1NF, 2NF, 3NF, BCNF, 4NF, 5NF:")
        while(choice not in ["1NF","2NF","3NF","BCNF","4NF","5NF"]):
            print("Entered normal form is invalid, please enter a valid normal form")
            choice = input("Enter the highest normal form to reach (1NF, 2NF, 3NF, BCNF, 4NF, 5NF:")
        mvd_dependencies=[]
        if choice in ["4NF","5NF"]:
            mvd_dependencies = parse_mvd_dependencies(mvd_file_path)  #path of mutlivalue dependency file
            print('MULTI-VALUED DEPENDENCIES')
            print(mvd_dependencies)
            print('\n')
        
        decomposition = get_normal_form_choice(parsed_data, functional_dependencies,choice,mvd_dependencies)
        # Print the generated queries
        

        #highest_normal_form = find_highest_normal_form(parsed_data, functional_dependencies)

        output_file_path = "outputl.txt"  # Replace with the desired output file path
        #output_query_path = "query.txt"
        write_to_text_file(parsed_data, output_file_path)

        print(f"Parsed Data has been written to {output_file_path}")
        #print(f"Highest Normal Form: {highest_normal_form}\n")
