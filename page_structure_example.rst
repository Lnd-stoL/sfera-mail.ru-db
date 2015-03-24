=============================
Oleg Tsarev TechnoSphera DBMS
=============================

Introduction
============

Here is home work of Oleg Tsarev DBMS course of Mail.Ru TechnoSphera.

I implemented simple search tree based on  `B*+-Tree <https://en.wikipedia.org/wiki/B%2B_tree>`_ (B-Plus-Star-Tree) algorithm.

This document explains the structure and implementation details with
references to source code.

.. contents:: Table of Contents

Storage file
============

All data stored in single **file**. The file splited to the fixed-size **pages** and
**page size** choosed during DB creation.
The page enumerated from zero and references each other over **page number**.

First page is **storage metadata**.
Other pages can be one of the following kind:

- **data page** which stored the DB pages (B+* tree nodes).

- **index page** which contains information about used and unused **data pages**.

Source is *storage.h*

Page in general
===============

All pages (**storage metadata**, **index page**, **data page**) has common structure:

+-------------+------+-------------------------------+
|        Name |Width | Description                   |
+=============+======+===============================+
|       MAGIC |32-bit| Magic constant ``0xFACABADA`` |
+-------------+------+-------------------------------+
|        SIZE |32-bit| Size of the page              |
+-------------+------+-------------------------------+
|       CRC32 |32-bit| CRC-32 checksum of page       |
+-------------+------+-------------------------------+
| PAGE NUMBER |32-bit| Flag which explain page kind  |
+-------------+------+-------------------------------+
|   PAGE KIND |32-bit| Flag which explain page kind  |
+-------------+------+-------------------------------+

The rest of the page depends from the page kind.

Source is *page.h*

---------------
Page kind flags
---------------

+-----------------------------+--------------+
|                   Page kind |        Value |
+=============================+==============+
|        **storage metadata** |``0xFFFFFFFF``|
+-----------------------------+--------------+
|              **index page** |``0xEEEEEEEE``|
+-----------------------------+--------------+
|          **data LEAF page** |``0xDDDDDDD0``|
+-----------------------------+--------------+
|      **data NOT-LEAF page** |``0xDDDDDDD1``|
+-----------------------------+--------------+
|     **data ROOT LEAF page** |``0xDDDDDDD2``|
+-----------------------------+--------------+
| **data ROOT NOT-LEAF page** |``0xDDDDDDD3``|
+-----------------------------+--------------+

Source is *page.h*


Storage metadata
================

**storage metadata** has two parts: **storage metadata header** and **index pages catalog**

-----------------------
Storage metadata header
-----------------------

All fields aligned to 32-bit offset.

+-------------------+------+----------------------------------+
|Name               |Width | Description                      |
+===================+======+==================================+
|             MAGIC |32-bit| See `Page in general`_           |
+-------------------+------+----------------------------------+
|              SIZE |16-bit| See `Page in general`_           |
+-------------------+------+----------------------------------+
|            CRC-32 |32-bit| See `Page in general`_           |
+-------------------+------+----------------------------------+
|       PAGE NUMBER |32-bit| See `Page in general`_           |
+-------------------+------+----------------------------------+
|         PAGE KIND |32-bit| See `Page kind flags`_           |
+-------------------+------+----------------------------------+
|           DB SIZE |32-bit| Maximum size (Kb) of storage     |
+-------------------+------+----------------------------------+
|         PAGE_SIZE |16-bit| Size (b) of node/page of tree    |
+-------------------+------+----------------------------------+
|  ROOT PAGE NUMBER |32-bit| Page number of the B+*-tree root |
+-------------------+------+----------------------------------+
| INDEX PAGES COUNT |32-bit| Count of index pages (see below) |
+-------------------+------+----------------------------------+
|          INDEX #0 |32-bit| 1st index page number            |
+-------------------+------+----------------------------------+
|          INDEX #1 |32-bit| 2nd index page number            |
+-------------------+------+----------------------------------+
|               ... |32-bit| ...                              |
+-------------------+------+----------------------------------+
|          INDEX #N |32-bit| N = INDEX PAGE COUNT             |
+-------------------+------+----------------------------------+

Source is *metadata.h*

-------------------
Index pages catalog
-------------------

The rest of the page is array of **page numbers** which reference to **index pages**

Source is *metadata.h*

Index Page
==========

Every **index page** contains two parts:

- **index page header**

- **index page data**

Source is *index.h*

-----------------
Index Page header
-----------------

TODO

Source is *index.h*

---------------
Index Page Data
---------------

Index Page Data is bitset, where every bit

- ``0`` if page used by storage
- ``1`` if page is free and can be reused

Source is *index.h*

Data Page
=========

Every data page can be two kinds:

- **leaf** page

- **not-leaf** page

More over, page can has **root** flag  which indicates root page.
Every page has **data page header** and **data page data** parts

Source is *node.h*

----------------
Data Page Header
----------------

TODO

Source is *node.h*

--------------
Leaf Data Page
--------------

TODO

Source is *node.h*

------------------
Not-Leaf Data Page
------------------

TODO

Source is *node.h*
