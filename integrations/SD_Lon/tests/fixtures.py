import typing
from collections import OrderedDict
from typing import Any
from typing import List
from typing import Tuple
from uuid import uuid4

from ra_utils.attrdict import AttrDict
from ra_utils.attrdict import attrdict


def get_sd_person_fixture(
    cpr: str, first_name: str, last_name: str, employment_id: str
) -> Tuple[AttrDict, List[typing.OrderedDict[str, Any]]]:
    """
    Get an SD person fixture. The function generates both the XML response
    returned from the SD API endpoint "GetPerson20111201" and the expected
    `OrderedDict` after parsing the XML.

    Args:
        cpr: The CPR number of the SD person.
        first_name: The first name (given name) of the SD person.
        last_name: The last name (surname) of the SD person.
        employment_id: The employmentID, e.g. 12345, of the SD person.

    Returns:
        Tuple with two elements. The first element is the raw XML response
        from the "GetPerson20111201" SD endpoint. The second element is the
        `OrderedDict` expected to be returned from get_sd_person.

    Example:
        ```
        >>> fix = get_sd_person_fixture('123456-1234', 'Bruce', 'Lee', "12345")
        >>> print(fix[0].text)
            <GetPerson20111201 creationDateTime="2020-12-03T17:40:10">
                <RequestStructure>
                    <InstitutionIdentifier>XX</InstitutionIdentifier>
                    <PersonCivilRegistrationIdentifier>123456-1234</PersonCivilRegistrationIdentifier>
                    <EffectiveDate>2020-12-03</EffectiveDate>
                    <StatusActiveIndicator>true</StatusActiveIndicator>
                    <StatusPassiveIndicator>false</StatusPassiveIndicator>
                    <ContactInformationIndicator>false</ContactInformationIndicator>
                    <PostalAddressIndicator>false</PostalAddressIndicator>
                </RequestStructure>
                <Person>
                    <PersonCivilRegistrationIdentifier>123456-1234</PersonCivilRegistrationIdentifier>
                    <PersonGivenName>Bruce</PersonGivenName>
                    <PersonSurnameName>Lee</PersonSurnameName>
                    <Employment>
                        <EmploymentIdentifier>12345</EmploymentIdentifier>
                    </Employment>
                </Person>
            </GetPerson20111201>
        >>> print(fix[1])
            [
                OrderedDict([
                    ('PersonCivilRegistrationIdentifier', '123456-1234'),
                    ('PersonGivenName', 'Bruce'),
                    ('PersonSurnameName', 'Lee'),
                    ('Employment', OrderedDict([
                        ('EmploymentIdentifier', '12345')
                    ]))
                ])
            ]
        ```
    """

    institution_id = "XX"

    sd_request_reply = attrdict(
        {
            "text": f"""
        <GetPerson20111201 creationDateTime="2020-12-03T17:40:10">
            <RequestStructure>
                <InstitutionIdentifier>{institution_id}</InstitutionIdentifier>
                <PersonCivilRegistrationIdentifier>{cpr}</PersonCivilRegistrationIdentifier>
                <EffectiveDate>2020-12-03</EffectiveDate>
                <StatusActiveIndicator>true</StatusActiveIndicator>
                <StatusPassiveIndicator>false</StatusPassiveIndicator>
                <ContactInformationIndicator>false</ContactInformationIndicator>
                <PostalAddressIndicator>false</PostalAddressIndicator>
            </RequestStructure>
            <Person>
                <PersonCivilRegistrationIdentifier>{cpr}</PersonCivilRegistrationIdentifier>
                <PersonGivenName>{first_name}</PersonGivenName>
                <PersonSurnameName>{last_name}</PersonSurnameName>
                <Employment>
                    <EmploymentIdentifier>{employment_id}</EmploymentIdentifier>
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


def read_employment_fixture(
    cpr: str, employment_id: str, job_id: str, job_title: str, status: str = "1"
) -> Tuple[AttrDict, List[typing.OrderedDict[str, Any]]]:
    """
    Get an SD employment fixture. The function is use for mocking calls to
    the "GetEmploymentChangedAtDate20111201" SD API endpoint, i.e. the endpoint
    that gets new changes *registered* between a from date and a to date. The
    function generates both the XML response endpoint
    "GetEmploymentChangedAtDate20111201" and the expected `OrderedDict` after
    parsing the XML.

    Args:
        cpr: The CPR number of the SD person.
        employment_id: The SD employment ID.
        job_id: The SD job ID.
        job_title: The SD profession.
        status: SD employment status.

    Returns:
        Tuple with two elements. The first element is the raw XML response
        from the "GetEmploymentChangedAtDate20111201" SD endpoint. The second
        element is the `OrderedDict` expected to be returned from
        read_employment_changed.

    Example:
        ```
        >>> fix=read_employment_fixture("123456-1234", "12345", "1", "chief", "1")
        >>> print(fix[1])
            [
                OrderedDict([
                    ('PersonCivilRegistrationIdentifier', '123456-1234'),
                    ('Employment', OrderedDict([
                        ('EmploymentIdentifier', '12345'),
                        ('EmploymentDate', '2020-11-10'),
                        ('AnniversaryDate', '2004-08-15'),
                        ('EmploymentDepartment', OrderedDict([
                            ('@changedAtDate', '2020-11-10'),
                            ('ActivationDate', '2020-11-10'),
                            ('DeactivationDate', '9999-12-31'),
                            ('DepartmentIdentifier', 'deprtment_id'),
                            ('DepartmentUUIDIdentifier', 'department_uuid')
                        ])),
                        ('Profession', OrderedDict([
                            ('@changedAtDate', '2020-11-10'),
                            ('ActivationDate', '2020-11-10'),
                            ('DeactivationDate', '9999-12-31'),
                            ('JobPositionIdentifier', '1'),
                            ('EmploymentName', 'chief'),
                            ('AppointmentCode', '0')
                        ])),
                        ('EmploymentStatus', [
                            OrderedDict([
                                ('@changedAtDate', '2020-11-10'),
                                ('ActivationDate', '2020-11-10'),
                                ('DeactivationDate', '2021-02-09'),
                                ('EmploymentStatusCode', '1')
                            ]),
                            OrderedDict([
                                ('@changedAtDate', '2020-11-10'),
                                ('ActivationDate', '2021-02-10'),
                                ('DeactivationDate', '9999-12-31'),
                                ('EmploymentStatusCode', '8')
                            ])
                        ])
                    ]))
                ])
            ]
        ```
    """
    institution_id = "institution_id"
    department_id = "deprtment_id"
    department_uuid = "department_uuid"

    sd_request_structure = f"""
        <RequestStructure>
            <InstitutionIdentifier>{institution_id}</InstitutionIdentifier>
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
    sd_request_person_employeed = f"""
        <Person>
            <PersonCivilRegistrationIdentifier>{cpr}</PersonCivilRegistrationIdentifier>
            <Employment>
                <EmploymentIdentifier>{employment_id}</EmploymentIdentifier>
                <EmploymentDate>2020-11-10</EmploymentDate>
                <AnniversaryDate>2004-08-15</AnniversaryDate>
                <EmploymentDepartment changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <DepartmentIdentifier>{department_id}</DepartmentIdentifier>
                    <DepartmentUUIDIdentifier>{department_uuid}</DepartmentUUIDIdentifier>
                </EmploymentDepartment>
                <Profession changedAtDate="2020-11-10">
                    <ActivationDate>2020-11-10</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <JobPositionIdentifier>{job_id}</JobPositionIdentifier>
                    <EmploymentName>{job_title}</EmploymentName>
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
    employeed_result = OrderedDict(
        [
            ("PersonCivilRegistrationIdentifier", cpr),
            (
                "Employment",
                OrderedDict(
                    [
                        ("EmploymentIdentifier", employment_id),
                        ("EmploymentDate", "2020-11-10"),
                        ("AnniversaryDate", "2004-08-15"),
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
    sd_request_person_deleted = f"""
        <Person>
            <PersonCivilRegistrationIdentifier>{cpr}</PersonCivilRegistrationIdentifier>
            <Employment>
                <EmploymentIdentifier>{employment_id}</EmploymentIdentifier>
                <EmploymentStatus changedAtDate="2020-11-09">
                    <ActivationDate>2020-11-01</ActivationDate>
                    <DeactivationDate>9999-12-31</DeactivationDate>
                    <EmploymentStatusCode>S</EmploymentStatusCode>
                </EmploymentStatus>
            </Employment>
        </Person>
    """
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


def get_department_fixture(
    institution_id="XX",
    institution_uuid=None,
    region_id="XY",
    region_uuid=None,
    department1_id="D1X",
    department1_uuid=None,
    department1_name="D1X-name",
    sub_department1_id="D1Y",
    sub_department1_uuid=None,
    sub_department1_name="D1Y-name",
    department2_id="D2X",
    department2_uuid=None,
    department2_name="D2X-name",
    sub_department2_id="D2Y",
    sub_department2_uuid=None,
    sub_department2_name="D2Y-name",
):
    institution_uuid = institution_uuid or str(uuid4())
    region_uuid = region_uuid or str(uuid4())

    department1_uuid = department1_uuid or str(uuid4())
    sub_department1_uuid = sub_department1_uuid or str(uuid4())

    department2_uuid = department2_uuid or str(uuid4())
    sub_department2_uuid = sub_department2_uuid or str(uuid4())

    return attrdict(
        {
            "text": f"""
        <GetDepartment20111201 creationDateTime="2021-10-20T14:51:23">
        <RequestStructure>
            <InstitutionIdentifier>{institution_id}</InstitutionIdentifier>
            <ActivationDate>2021-10-20</ActivationDate>
            <DeactivationDate>2021-10-20</DeactivationDate>
            <ContactInformationIndicator>true</ContactInformationIndicator>
            <DepartmentNameIndicator>true</DepartmentNameIndicator>
            <EmploymentDepartmentIndicator>true</EmploymentDepartmentIndicator>
            <PostalAddressIndicator>true</PostalAddressIndicator>
            <ProductionUnitIndicator>true</ProductionUnitIndicator>
            <UUIDIndicator>true</UUIDIndicator>
        </RequestStructure>
        <RegionIdentifier>{region_id}</RegionIdentifier>
        <RegionUUIDIdentifier>{region_uuid}</RegionUUIDIdentifier>
        <InstitutionIdentifier>{institution_id}</InstitutionIdentifier>
        <InstitutionUUIDIdentifier>{institution_uuid}</InstitutionUUIDIdentifier>
        <Department>
            <ActivationDate>2010-01-01</ActivationDate>
            <DeactivationDate>9999-12-31</DeactivationDate>
            <DepartmentIdentifier>{department1_id}</DepartmentIdentifier>
            <DepartmentUUIDIdentifier>{department1_uuid}</DepartmentUUIDIdentifier>
            <DepartmentLevelIdentifier>Afdelings-niveau</DepartmentLevelIdentifier>
            <DepartmentName>{department1_name}</DepartmentName>
            <PostalAddress>
                <StandardAddressIdentifier>
                    Department 1 Address
                </StandardAddressIdentifier>
                <PostalCode>8600</PostalCode>
                <DistrictName>Silkeborg</DistrictName>
                <MunicipalityCode>0740</MunicipalityCode>
            </PostalAddress>
        </Department>
        <Department>
            <ActivationDate>2011-01-01</ActivationDate>
            <DeactivationDate>9999-12-31</DeactivationDate>
            <DepartmentIdentifier>{sub_department1_id}</DepartmentIdentifier>
            <DepartmentUUIDIdentifier>{sub_department1_uuid}</DepartmentUUIDIdentifier>
            <DepartmentLevelIdentifier>NY5-niveau</DepartmentLevelIdentifier>
            <DepartmentName>{sub_department1_name}</DepartmentName>
            <PostalAddress>
                <StandardAddressIdentifier>
                    Sub Department 1 Address
                </StandardAddressIdentifier>
                <PostalCode>8600</PostalCode>
                <DistrictName>Silkeborg</DistrictName>
                <MunicipalityCode>0740</MunicipalityCode>
            </PostalAddress>
            <ContactInformation>
                <EmailAddressIdentifier>sub_department_1@example.org</EmailAddressIdentifier>
                <EmailAddressIdentifier>Empty@Empty</EmailAddressIdentifier>
            </ContactInformation>
        </Department>
        <Department>
            <ActivationDate>2012-01-01</ActivationDate>
            <DeactivationDate>9999-12-31</DeactivationDate>
            <DepartmentIdentifier>{department2_id}</DepartmentIdentifier>
            <DepartmentUUIDIdentifier>{department2_uuid}</DepartmentUUIDIdentifier>
            <DepartmentLevelIdentifier>Afdelings-niveau</DepartmentLevelIdentifier>
            <DepartmentName>{department2_name}</DepartmentName>
            <PostalAddress>
                <StandardAddressIdentifier>
                    Department 2 Address
                </StandardAddressIdentifier>
                <PostalCode>8600</PostalCode>
                <DistrictName>Silkeborg</DistrictName>
                <MunicipalityCode>0740</MunicipalityCode>
            </PostalAddress>
        </Department>
        <Department>
            <ActivationDate>2013-01-01</ActivationDate>
            <DeactivationDate>9999-12-31</DeactivationDate>
            <DepartmentIdentifier>{sub_department2_id}</DepartmentIdentifier>
            <DepartmentUUIDIdentifier>{sub_department2_uuid}</DepartmentUUIDIdentifier>
            <DepartmentLevelIdentifier>NY5-niveau</DepartmentLevelIdentifier>
            <DepartmentName>{sub_department2_name}</DepartmentName>
            <PostalAddress>
                <StandardAddressIdentifier>
                    Sub Department 2 Address
                </StandardAddressIdentifier>
                <PostalCode>8600</PostalCode>
                <DistrictName>Silkeborg</DistrictName>
                <MunicipalityCode>0740</MunicipalityCode>
            </PostalAddress>
            <ContactInformation>
                <EmailAddressIdentifier>sub_department_2@example.org</EmailAddressIdentifier>
                <EmailAddressIdentifier>Empty@Empty</EmailAddressIdentifier>
            </ContactInformation>
        </Department>
        </GetDepartment20111201>
        """
        }
    )


def get_organisation_fixture(
    institution_id="XX",
    institution_uuid=None,
    region_id="XY",
    region_uuid=None,
    department_structure_name="XX-Basis",
    department1_id="D1X",
    department1_uuid=None,
    sub_department1_id="D1Y",
    sub_department1_uuid=None,
    department2_id="D2X",
    department2_uuid=None,
    sub_department2_id="D2Y",
    sub_department2_uuid=None,
):
    institution_uuid = institution_uuid or str(uuid4())
    region_uuid = region_uuid or str(uuid4())

    department1_uuid = department1_uuid or str(uuid4())
    sub_department1_uuid = sub_department1_uuid or str(uuid4())

    department2_uuid = department2_uuid or str(uuid4())
    sub_department2_uuid = sub_department2_uuid or str(uuid4())

    return attrdict(
        {
            "text": f"""
        <GetOrganization20111201 creationDateTime="2021-10-20T14:35:44">
            <RequestStructure>
                <InstitutionIdentifier>{institution_id}</InstitutionIdentifier>
                <ActivationDate>2021-10-20</ActivationDate>
                <DeactivationDate>2021-10-20</DeactivationDate>
                <UUIDIndicator>true</UUIDIndicator>
            </RequestStructure>
            <RegionIdentifier>{region_id}</RegionIdentifier>
            <RegionUUIDIdentifier>{region_uuid}</RegionUUIDIdentifier>
            <InstitutionIdentifier>{institution_id}</InstitutionIdentifier>
            <InstitutionUUIDIdentifier>{institution_uuid}</InstitutionUUIDIdentifier>
            <DepartmentStructureName>{department_structure_name}</DepartmentStructureName>
            <OrganizationStructure>
                <DepartmentLevelReference>
                    <DepartmentLevelIdentifier>Afdelings-niveau</DepartmentLevelIdentifier>
                    <DepartmentLevelReference>
                        <DepartmentLevelIdentifier>NY0-niveau</DepartmentLevelIdentifier>
                        <DepartmentLevelReference>
                            <DepartmentLevelIdentifier>NY1-niveau</DepartmentLevelIdentifier>
                            <DepartmentLevelReference>
                                <DepartmentLevelIdentifier>NY2-niveau</DepartmentLevelIdentifier>
                                <DepartmentLevelReference>
                                    <DepartmentLevelIdentifier>NY3-niveau</DepartmentLevelIdentifier>
                                    <DepartmentLevelReference>
                                        <DepartmentLevelIdentifier>NY4-niveau</DepartmentLevelIdentifier>
                                        <DepartmentLevelReference>
                                            <DepartmentLevelIdentifier>NY5-niveau</DepartmentLevelIdentifier>
                                            <DepartmentLevelReference>
                                                <DepartmentLevelIdentifier>NY6-niveau</DepartmentLevelIdentifier>
                                            </DepartmentLevelReference>
                                        </DepartmentLevelReference>
                                    </DepartmentLevelReference>
                                </DepartmentLevelReference>
                            </DepartmentLevelReference>
                        </DepartmentLevelReference>
                    </DepartmentLevelReference>
                </DepartmentLevelReference>
            </OrganizationStructure>
            <Organization>
                <ActivationDate>2021-10-01</ActivationDate>
                <DeactivationDate>2021-10-20</DeactivationDate>
                <DepartmentReference>
                    <DepartmentIdentifier>{department1_id}</DepartmentIdentifier>
                    <DepartmentUUIDIdentifier>{department1_uuid}</DepartmentUUIDIdentifier>
                    <DepartmentLevelIdentifier>Afdelings-niveau</DepartmentLevelIdentifier>
                    <DepartmentReference>
                        <DepartmentIdentifier>{sub_department1_id}</DepartmentIdentifier>
                        <DepartmentUUIDIdentifier>{sub_department1_uuid}</DepartmentUUIDIdentifier>
                        <DepartmentLevelIdentifier>NY5-niveau</DepartmentLevelIdentifier>
                    </DepartmentReference>
                </DepartmentReference>
                <DepartmentReference>
                    <DepartmentIdentifier>{department2_id}</DepartmentIdentifier>
                    <DepartmentUUIDIdentifier>{department2_uuid}</DepartmentUUIDIdentifier>
                    <DepartmentLevelIdentifier>Afdelings-niveau</DepartmentLevelIdentifier>
                    <DepartmentReference>
                        <DepartmentIdentifier>{sub_department2_id}</DepartmentIdentifier>
                        <DepartmentUUIDIdentifier>{sub_department2_uuid}</DepartmentUUIDIdentifier>
                        <DepartmentLevelIdentifier>NY5-niveau</DepartmentLevelIdentifier>
                    </DepartmentReference>
                </DepartmentReference>
            </Organization>
        </GetOrganization20111201>
        """
        }
    )


def get_employment_fixture(
    cpr, employment_id, department_id, department_uuid, job_pos_id, job_title
):
    """
    Get the OrderedDict returned when sd_common.sd_lookup is called for the
    SD endpoint GetEmployment20111201.
    """

    return OrderedDict(
        [
            (
                "Person",
                OrderedDict(
                    [
                        ("PersonCivilRegistrationIdentifier", cpr),
                        (
                            "Employment",
                            OrderedDict(
                                [
                                    ("EmploymentIdentifier", employment_id),
                                    ("EmploymentDate", "2020-11-10"),
                                    ("AnniversaryDate", "2004-08-15"),
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
                                                ("JobPositionIdentifier", job_pos_id),
                                                ("EmploymentName", job_title),
                                                ("AppointmentCode", "0"),
                                            ]
                                        ),
                                    ),
                                    (
                                        "EmploymentStatus",
                                        OrderedDict(
                                            [
                                                ("@changedAtDate", "2020-11-10"),
                                                ("ActivationDate", "2020-11-10"),
                                                ("DeactivationDate", "2021-02-09"),
                                                ("EmploymentStatusCode", "1"),
                                            ]
                                        ),
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            )
        ]
    )


def get_read_employment_changed_fixture(
    cpr: int = 1234561234,
    employment_id: str = "12345",
    employment_date: str = "2020s-01-01",
    anniversary_date: str = "2020-01-01",
    employment_department_activation_date: str = "2020-01-01",
    employment_department_deactivation_date: str = "9999-12-31",
    department_id: str = "department_id",
    department_uuid_id: str = "department_uuid",
    profession_activation_date: str = "2020-01-01",
    profession_deactivation_date: str = "9999-12-31",
    job_pos_id: int = 1000,
    employment_name: str = "Employment name",
    appointment_code: str = "0",
    employment_status_activation_date: str = "2020-01-01",
    employment_status_deactivation_date: str = "9999-12-31",
    employment_status_code: int = 1,
):
    return [
        OrderedDict(
            [
                ("PersonCivilRegistrationIdentifier", str(cpr)),
                (
                    "Employment",
                    OrderedDict(
                        [
                            ("EmploymentIdentifier", employment_id),
                            ("EmploymentDate", employment_date),
                            ("AnniversaryDate", anniversary_date),
                            (
                                "EmploymentDepartment",
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        (
                                            "ActivationDate",
                                            employment_department_activation_date,
                                        ),
                                        (
                                            "DeactivationDate",
                                            employment_department_deactivation_date,
                                        ),
                                        ("DepartmentIdentifier", department_id),
                                        (
                                            "DepartmentUUIDIdentifier",
                                            department_uuid_id,
                                        ),
                                    ]
                                ),
                            ),
                            (
                                "Profession",
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        ("ActivationDate", profession_activation_date),
                                        (
                                            "DeactivationDate",
                                            profession_deactivation_date,
                                        ),
                                        ("JobPositionIdentifier", str(job_pos_id)),
                                        ("EmploymentName", employment_name),
                                        ("AppointmentCode", appointment_code),
                                    ]
                                ),
                            ),
                            (
                                "EmploymentStatus",
                                OrderedDict(
                                    [
                                        ("@changedAtDate", "2020-11-10"),
                                        (
                                            "ActivationDate",
                                            employment_status_activation_date,
                                        ),
                                        (
                                            "DeactivationDate",
                                            employment_status_deactivation_date,
                                        ),
                                        (
                                            "EmploymentStatusCode",
                                            str(employment_status_code),
                                        ),
                                    ]
                                ),
                            ),
                        ]
                    ),
                ),
            ]
        )
    ]
