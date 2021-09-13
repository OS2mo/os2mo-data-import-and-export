from collections import OrderedDict

from ra_utils.attrdict import attrdict


def read_person_fixture(cpr, first_name, last_name, employment_id):
    institution_id = "XX"

    sd_request_reply = attrdict(
        {
            "text": """
        <GetPerson20111201 creationDateTime="2020-12-03T17:40:10">
            <RequestStructure>
                <InstitutionIdentifier>"""
            + institution_id
            + """</InstitutionIdentifier>
                <PersonCivilRegistrationIdentifier>"""
            + cpr
            + """</PersonCivilRegistrationIdentifier>
                <EffectiveDate>2020-12-03</EffectiveDate>
                <StatusActiveIndicator>true</StatusActiveIndicator>
                <StatusPassiveIndicator>false</StatusPassiveIndicator>
                <ContactInformationIndicator>false</ContactInformationIndicator>
                <PostalAddressIndicator>false</PostalAddressIndicator>
            </RequestStructure>
            <Person>
                <PersonCivilRegistrationIdentifier>"""
            + cpr
            + """</PersonCivilRegistrationIdentifier>
                <PersonGivenName>"""
            + first_name
            + """</PersonGivenName>
                <PersonSurnameName>"""
            + last_name
            + """</PersonSurnameName>
                <Employment>
                    <EmploymentIdentifier>"""
            + employment_id
            + """</EmploymentIdentifier>
                </Employment>
            </Person>
        </GetPerson20111201>
        """
        }
    )

    expected_read_person_result = [
        OrderedDict(
            [
                ("PersonCivilRegistrationIdentifier", cpr),
                ("PersonGivenName", first_name),
                ("PersonSurnameName", last_name),
                (
                    "Employment",
                    OrderedDict([("EmploymentIdentifier", employment_id)]),
                ),
            ]
        )
    ]

    return sd_request_reply, expected_read_person_result


def read_employment_fixture(cpr, employment_id, job_id, job_title, status="1"):
    institution_id = "institution_id"
    department_id = "deprtment_id"
    department_uuid = "department_uuid"

    sd_request_structure = (
        """
        <RequestStructure>
            <InstitutionIdentifier>"""
        + institution_id
        + """</InstitutionIdentifier>
            <ActivationDate>2020-11-01</ActivationDate>
            <ActivationTime>00:00:00</ActivationTime>
            <DeactivationDate>2020-12-02</DeactivationDate>
            <DeactivationTime>23:59:59</DeactivationTime>
            <DepartmentIndicator>true</DepartmentIndicator>
            <EmploymentStatusIndicator>true</EmploymentStatusIndicator>
            <ProfessionIndicator>true</ProfessionIndicator>
            <SalaryAgreementIndicator>false</SalaryAgreementIndicator>
            <SalaryCodeGroupIndicator>false</SalaryCodeGroupIndicator>
            <WorkingTimeIndicator>false</WorkingTimeIndicator>
            <UUIDIndicator>true</UUIDIndicator>
            <FutureInformationIndicator>false</FutureInformationIndicator>
        </RequestStructure>
    """
    )
    sd_request_person_employeed = (
        """
        <Person>
            <PersonCivilRegistrationIdentifier>"""
        + cpr
        + """</PersonCivilRegistrationIdentifier>
            <Employment>
                <EmploymentIdentifier>"""
        + employment_id
        + """</EmploymentIdentifier>
                <EmploymentDate>2020-11-10</EmploymentDate>
                <EmploymentDepartment changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <DepartmentIdentifier>"""
        + department_id
        + """</DepartmentIdentifier>
                    <DepartmentUUIDIdentifier>"""
        + department_uuid
        + """</DepartmentUUIDIdentifier>
                </EmploymentDepartment>
                <Profession changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <JobPositionIdentifier>"""
        + job_id
        + """</JobPositionIdentifier>
                    <EmploymentName>"""
        + job_title
        + """</EmploymentName>
                    <AppointmentCode>0</AppointmentCode>
                </Profession>
                <EmploymentStatus changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>2021-02-09</DeactivationDate>
                    <EmploymentStatusCode>1</EmploymentStatusCode>
                </EmploymentStatus>
                <EmploymentStatus changedAtDate="2020-11-10">
                    <ActivationDate>2021-02-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <EmploymentStatusCode>8</EmploymentStatusCode>
                </EmploymentStatus>
            </Employment>
        </Person>
    """
    )
    employeed_result = OrderedDict(
        [
            ("PersonCivilRegistrationIdentifier", cpr),
            (
                "Employment",
                OrderedDict(
                    [
                        ("EmploymentIdentifier", employment_id),
                        ("EmploymentDate", "2020-11-10"),
                        (
                            "EmploymentDepartment",
                            OrderedDict(
                                [
                                    ("@changedAtDate", "2020-11-10"),
                                    ("ActivationDate", "2020-11-10"),
                                    ("DeactivationDate", "9999-12-31"),
                                    ("DepartmentIdentifier", department_id),
                                    (
                                        "DepartmentUUIDIdentifier",
                                        department_uuid,
                                    ),
                                ]
                            ),
                        ),
                        (
                            "Profession",
                            OrderedDict(
                                [
                                    ("@changedAtDate", "2020-11-10"),
                                    ("ActivationDate", "2020-11-10"),
                                    ("DeactivationDate", "9999-12-31"),
                                    ("JobPositionIdentifier", job_id),
                                    ("EmploymentName", job_title),
                                    ("AppointmentCode", "0"),
                                ]
                            ),
                        ),
                        (
                            "EmploymentStatus",
                            [
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        ("ActivationDate", "2020-11-10"),
                                        ("DeactivationDate", "2021-02-09"),
                                        ("EmploymentStatusCode", "1"),
                                    ]
                                ),
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        ("ActivationDate", "2021-02-10"),
                                        ("DeactivationDate", "9999-12-31"),
                                        ("EmploymentStatusCode", "8"),
                                    ]
                                ),
                            ],
                        ),
                    ]
                ),
            ),
        ]
    )
    sd_request_person_deleted = (
        """
        <Person>
            <PersonCivilRegistrationIdentifier>"""
        + cpr
        + """</PersonCivilRegistrationIdentifier>
            <Employment>
                <EmploymentIdentifier>"""
        + employment_id
        + """</EmploymentIdentifier>
                <EmploymentStatus changedAtDate="2020-11-09">
                    <ActivationDate>2020-11-01</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <EmploymentStatusCode>S</EmploymentStatusCode>
                </EmploymentStatus>
            </Employment>
        </Person>
    """
    )
    deleted_result = OrderedDict(
        [
            ("PersonCivilRegistrationIdentifier", cpr),
            (
                "Employment",
                OrderedDict(
                    [
                        ("EmploymentIdentifier", employment_id),
                        (
                            "EmploymentStatus",
                            OrderedDict(
                                [
                                    ("@changedAtDate", "2020-11-09"),
                                    ("ActivationDate", "2020-11-01"),
                                    ("DeactivationDate", "9999-12-31"),
                                    ("EmploymentStatusCode", "S"),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )

    person_table = {
        "1": (sd_request_person_employeed, employeed_result),
        "S": (sd_request_person_deleted, deleted_result),
    }
    sd_response = (
        """
        <GetEmploymentChangedAtDate20111201 creationDateTime="2020-12-02T16:44:19">
        """
        + sd_request_structure
        + person_table[status][0]
        + """
        </GetEmploymentChangedAtDate20111201>
    """
    )
    sd_request_reply = attrdict({"text": sd_response})
    expected_read_employment_result = [person_table[status][1]]
    return sd_request_reply, expected_read_employment_result
