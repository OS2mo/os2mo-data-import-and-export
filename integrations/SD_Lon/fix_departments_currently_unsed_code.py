# This file contains currently unused code that might turn out to be usefull
# if we choose to attempt to do a full historic import.


def create_department(self, department, activation_date):
    params = {
        'ActivationDate': activation_date,
        'DeactivationDate': activation_date,
        'DepartmentIdentifier': department['DepartmentIdentifier'],
        'ContactInformationIndicator': 'true',
        'DepartmentNameIndicator': 'true',
        'PostalAddressIndicator': 'false',
        'ProductionUnitIndicator': 'false',
        'UUIDIndicator': 'true',
        'EmploymentDepartmentIndicator': 'false'
    }
    department_info = sd_lookup('GetDepartment20111201', params)

    for unit_type in self.unit_types:
        if unit_type['user_key'] == department['DepartmentLevelIdentifier']:
            unit_type_uuid = unit_type['uuid']

    payload = sd_payloads.create_org_unit(
        department=department,
        org=self.org_uuid,
        name=department_info['Department']['DepartmentName'],
        unit_type=unit_type_uuid,
        from_date=department_info['Department']['ActivationDate']
    )
    logger.debug('Create department payload: {}'.format(payload))

    response = self.helper._mo_post('ou/create', payload)
    response.raise_for_status()
    logger.info('Created unit {}'.format(
        department['DepartmentIdentifier'])
    )
    logger.debug('Response: {}'.format(response.text))


def fix_departments(self):
    params = {
        'ActivationDate': self.from_date.strftime('%d.%m.%Y'),
        'DeactivationDate': '31.12.9999',
        'UUIDIndicator': 'true'
    }
    # TODO: We need to read this without caching!
    organisation = sd_lookup('GetOrganization20111201', params)
    department_lists = organisation['Organization']
    if not isinstance(department_lists, list):
        department_lists = [department_lists]

    total = 0
    checked = 0
    for department_list in department_lists:
        departments = department_list['DepartmentReference']
        total += len(departments)

    logger.info('Checking {} departments'.format(total))
    for department_list in department_lists:
        # All units in this list has same activation date
        activation_date = department_list['ActivationDate']

        departments = department_list['DepartmentReference']
        for department in departments:
            checked += 1
            print('{}/{} {:.2f}%'.format(checked, total, 100.0 * checked/total))
            departments = []
            uuid = department['DepartmentUUIDIdentifier']
            ou = self.helper.read_ou(uuid)
            logger.debug('Check for {}'.format(uuid))

            if 'status' in ou:  # Unit does not exist
                print('klaf')
                departments.append(department)
                logger.info('{} is new in MO'.format(uuid))
                parent_department = department
                while 'DepartmentReference' in parent_department:
                    parent_department = parent_department['DepartmentReference']
                    parent_uuid = parent_department['DepartmentUUIDIdentifier']
                    ou = self.helper.read_ou(parent_uuid)
                    logger.debug('Check for {}'.format(parent_uuid))
                    if 'status' in ou:  # Unit does not exist
                        logger.info('{} is new in MO'.format(parent_uuid))
                        departments.append(parent_department)

            # Create actual departments
            while departments:
                self.create_department(departments.pop(), activation_date)


def fix_specific_department(self, shortname):
    """
    Run through historic information from SD and attempt to replicate
    this in MO. Departments will not be created, so the existence in MO
    must be confirmed by other means.
    """
    # Semi-arbitrary start date for historic import
    from_date = datetime.datetime(2000, 1, 1, 0, 0)
    logger.info('Fix import of department {}'.format(shortname))

    params = {
        'ActivationDate': from_date.strftime('%d.%m.%Y'),
        'DeactivationDate': '9999-12-31',
        'DepartmentIdentifier': shortname,
        'UUIDIndicator': 'true',
        'DepartmentNameIndicator': 'true'
    }

    department = sd_lookup('GetDepartment20111201', params, use_cache=False)
    validities = department['Department']
    if isinstance(validities, dict):
        validities = [validities]

    first_iteration = True
    for validity in validities:
        assert shortname == validity['DepartmentIdentifier']
        validity_date = datetime.datetime.strptime(validity['ActivationDate'],
                                                   '%Y-%m-%d')
        user_key = shortname
        name = validity['DepartmentName']
        unit_uuid = validity['DepartmentUUIDIdentifier']

        for unit_level in self.level_types:
            if unit_level['user_key'] == validity['DepartmentLevelIdentifier']:
                unit_level_uuid = unit_level['uuid']

        # SD has a challenge with the internal validity-consistency, extend first
        # validity indefinitely
        if first_iteration:
            from_date = '1930-01-01'
            first_iteration = False
        else:
            from_date = validity_date.strftime('%Y-%m-%d')

        try:
            parent = self.get_parent(unit_uuid, datetime.datetime.now())
        except NoCurrentValdityException:
            print('Error')
            parent = self.settings[
                'integrations.SD_Lon.unknown_parent_container'
            ]
        print('Unit parent at {} is {}'.format(from_date, parent))

        payload = sd_payloads.edit_org_unit(
            user_key=user_key,
            name=name,
            unit_uuid=unit_uuid,
            parent=parent,
            ou_level=unit_level_uuid,
            ou_type=self.unit_type['uuid'],
            from_date=from_date
        )

        logger.debug('Edit payload to fix unit: {}'.format(payload))
        response = self.helper._mo_post('details/edit', payload)
        if response.status_code == 400:
            assert(response.text.find('raise to a new registration') > 0)
        else:
            response.raise_for_status()
        logger.debug('Response: {}'.format(response.text))
