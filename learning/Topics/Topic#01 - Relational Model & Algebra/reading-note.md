# Chapter 2    Introduction to the Relational Model

## 2.1 Structure of Relational Database

Relational database consists of *tables(relations)*. Each table has *attributes* and lines(*tuples*). Each tuple is called a *relation instance*. The set of permitted values of one attribute is called the *domain* of the attribute.

## 2.2 Database Schema

Database schema VS Database instance.

* Database schema: the logical design of the database.

* Database instance: which is a snapshot of the data in the database at a given instant in time.

Relation instance VS Relation schema

* Relation instance might change as the time goes.

* Relation schema (consists of a list of attributes and their corresponding domains) is often stable for a long time.

## 2.3 Keys

Types of keys

* superkey

* candidate key

* primary key

* foreign key

## 2.4 Schema Diagram

A database schema, along with primary key and foreign key dependencies, can

be depicted by *schema diagrams*.

## 2.5 Relational Query Languages

*Query language* is a language in which a user requests information from the

database. It can be categorized as either procedural language or nonprocedural language.

* procedural language: the user instructs the system to perform a sequence of operations on the database to compute the desired result.

* nonprocedural language: the user describes the desired information without giving a specifific procedure for obtaining that information.

## 2.6 Relational Operations

Their result is always a *single* relation. The common operations:

* Natural join

* Cartesian product
