# Lecture #01: Relational Model & Algebra

---

## 1 Database

---

*Database* is an organized collection of inter-related data that models some aspects the real world. *Database Management System(DBMS)* is the software that manages a database.

*Data model* is a collection of concepts for describing the data in the databse. 

Example: relational (most common), NoSQL (key/value, document, graph), array/matrix/vectors (for machine learning)

*Schema* is a description of a particular collection of data based on a data model.

## 2 Relational Model

---

Relational model has three key points: 

* Store databse in simple data structures(relation).

* Access data through high-level language, DBMS figures out the best execution strategy.

* Physical storage left up to the DBMS implementation.

The relational model defines the three concepts:

* **Structure**: The definition of the structures and their contents. This is the attributes the relations have and the values the attributes can hold.

* **Integrity**: Ensure the database's contents satisfy some constraints. 

* **Manipulation**: How to access and modify a database's contents.

A *relation* is an unordered set that contains the relationship of attributes that represent entities.

A *tuple* is a set of attribute values (also known as its domain) in the relation.

### Keys

A relation’s *primary key* uniquely identifies a single tuple.

A *foreign key* specifies that an attribute from one relation has to map to a tuple in another relation.

### Constraints

A constraint is a user-defined condition that must hold for any instance of the database

### Data Manipulation Langauges

Methods to store and retrieve information from database.

* Procedural: The query specifies the (high-level) strategy the DBMS should use to find the desired result based on sets / bags.

* Non-Procedural (Declarative): The query specifies only what data is wanted and not how to find it.  

## 3 Relational Algebra

---

*Relational Algebra* is a set of fundamental operations to retrieve and manipulate tuples in a relation. These are the common used operations:

* Select

* Projection

* Union

* Intersection

* Difference

* Product

* Join

## 4 Alternative Data Models

---

* **Document Model**: collection of record documents containing a hierarchy of named field/value pairs.

* **Vector Model**: one-dimensional arrays used for nearest-neighbor search (exact or approximate).
