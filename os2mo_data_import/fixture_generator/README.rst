Dummy data creator
******************

This application will generate tree structure containing a mock-up of a Danish
municipality. The organisation is randomized but modelled to be somewhat realistic.
The model is provided with a municipality code and this information is used to
provided addresses and also to create fictional schools and kindergartens based on the
postal areas in the municipality.

Classification
==============
A number of classes are provided, they are partly used to create the fictional users
and units, but also to allow the user interface to work in a reasonable realistic way
after the initial creation of the structure.

Unit properties
===============
In the default state, all units will have exactly on manager and a number of users.
The number of users will on average be higher in the deeper parts of the tree. A
scale factor is provided to allow for as few or as many users as wanted. A scale
factor of one, will create a very small tree, possible some units will have no
employees at all. A scale factor of 4 will yield an organization of ~700-800 users.

User properties
===============
In the default configuration, all users will have exactly one engagement, which will
have high probability to be currently active. A smaller fraction of the engagements
will be either in the past or in the future, this allows for tests and demonstrations
of the bitemporality. All uses are equipped with a Danish address (given as DAR uuid
as well as plain text) and an email.

The users will have a certain probability to have a role, no users will have more
than one role. Also, the users will have probability to be associated with another
unit.

Most, but not all, users will have at least one account in an IT-system.

Performance stress test
=======================
The two main functions ``create_org_func_tree`` and ``add_users_to_tree`` both takes
arguments that will produce a very large but much less realistic, dataset. The main
purpose of this is to create datasets that can be used for performance tests.

Specifically, if ``create_org_func_tree`` is call with ``org_size`` set to
``Size.Large``, the structure will contain 25 copies of each school.
If ``add_users_to_tree`` is called with ``multiple_employments`` set to ``True``,
each employee will have a large number of engagements, typically both in the past
and in the future, and for most employees also in the present time.

Data structure
==============
The data is stored in an ``AnyTree`` data-structure. The tree will currently always
have either one root (default name ``root``), or two roots, employees will always be
leafs in the tree, some OUs might possibly have no employees in which case a unit
is a leaf. All nodes have a type that can be either ``ou`` or ``user``.

The data is read by traversing the tree. The ``manager`` field contains a list of
responsibilities if the person is a manager, otherwise an empty list.

.. code-block:: python

    if node.type == 'ou':
	print(node.name)  # Name of the ou
	print(node.adresse['dar-uuid'])
	if node.parent:
	    print(node.parent.key)  # Key for parent unit, root will have no parent

	    if node.type == 'user':
		print(node.name)  # Name of the employee
		print(node.parent.key) # Key to the users unit
		user = node.user  # All unser information is here
		print(user[0]['cpr']) # user_key and cpr are identical in all elements
		print(user[0]['brugervendtnoegle'])
		for engagement in user: # An element for each engagement
		    print(engagement['adresse']['dar-uuid'])
		    print(engagement['email'])
		    print(engagement['telefon'])
		    print('engagement['role']))
		    print(engagement['association'])
		    print(engagement['manager'])
		    print('From: {}. To: {}'.format(engagement['fra'],
						    engagement['til']))

Importing into MO
=================
If the user should want to import the structure into MO, this can be done with the
script ``populate_mo.py``.
