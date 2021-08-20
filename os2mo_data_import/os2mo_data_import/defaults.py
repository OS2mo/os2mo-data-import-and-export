#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

"""
The OS2MO application requires a fixed set of "Facet" objects.
By default the following types are automatically created:

    - org_unit_address_type
    - employee_address_type
    - manager_address_type
    - address_property
    - engagement_job_function
    - association_job_function
    - org_unit_type
    - engagement_type
    - association_type
    - role_type
    - leave_type
    - manager_type
    - responsibility
    - manager_level
    - visibility
    - time_planning
    - org_unit_level
    - primary_type
"""

facet_defaults = [
    "org_unit_address_type",
    "employee_address_type",
    "manager_address_type",
    "address_property",
    "engagement_job_function",
    "org_unit_type",
    "engagement_type",
    "association_type",
    "role_type",
    "leave_type",
    "manager_type",
    "responsibility",
    "manager_level",
    "visibility",
    "time_planning",
    "org_unit_level",
    "primary_type",
    "org_unit_hierarchy"
]
