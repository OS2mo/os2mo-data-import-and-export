**********************
Shared Reports
**********************

Shared reports introduces a collection of reports that can be used by customers regardless of their individual setup. They utilise standard MO-data and return `pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_, which can then be exported to a variety of file formats and served in ``/forespoergsler``.

Quick Start
=============
The ``main()`` method in ``/reports/shared_reports.py`` will generate all available reports. It utilises settings from ``settings.json``, specifically ``mora.base`` as the hostname, ``reports.org_name`` as the name of the organisation for which to generate reports, ``reports.pay_org_name`` as the name of the organisation from which to generate payroll reports, and ``mora.folder.query_export`` as the output directory.

.. admonition:: Example
    
    In ``settings.json``, the following settings should be available:

    .. code-block:: json

        {
            "mora.base": "http://localhost:5000",
            "mora.folder.query_export": "/opt/reports/"
            "reports.org_name": "Testkommune",
            "reports.pay_org_name": "Testkommune"
        }
    
    Note that the organisation name and the payroll organisation name are identical. This will be the case for most customers, but some use a different payroll organisation, and thus we need to be able to specify this setting.

    Then, to generate all reports in CSV-format, simply call

    .. code-block:: bash

        python /reports/shared_reports.py

If only a subset of reports and/or different output formats are required, the API can be used directly -- refer to the following section.


API Reference
=============

.. autoclass:: reports.shared_reports.CustomerReports
    :members:
    :special-members:



