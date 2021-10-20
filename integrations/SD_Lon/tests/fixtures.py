from collections import OrderedDict
from uuid import uuid4

from ra_utils.attrdict import attrdict


def read_person_fixture(cpr, first_name, last_name, employment_id):
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


def read_employment_fixture(cpr, employment_id, job_id, job_title, status="1"):
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
