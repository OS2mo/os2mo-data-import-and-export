from functools import lru_cache

from ra_utils.job_settings import JobSettings


class EmployeePhoneBookSettings(JobSettings):
    # common settings for clients:
    ftp_url: str | None
    ftp_port: int | None
    ftp_user: str | None
    ftp_pass: str | None
    ftp_ssh_key_path: str | None
    ftp_ssh_key_pass: str | None
    ftp_folder: str | None
    import_state_file: str | None
    import_csv_folder: str | None
    mox_base: str = "http://localhost:5000/lora"
    mora_base: str = "http://localhost:5000"
    report_dir_path: str = "/opt/docker/os2mo/queries"

    sql_cell_phone_number_field: str | None  # Desired cell phone type - "AD-Mobil".
    sql_phone_number_field: str | None  # Desired phone type - "AD-Telefonnummer".
    sql_visibility_scope_field: str | None  # Exclude visibility scope of - "SECRET".
    sql_excluded_organisation_units_user_key: str | None  # Exclude certain organisation units.
    sql_excluded_organisation_units_uuid: str | None  # Exclude certain organisation units by uuid.


@lru_cache()
def get_employee_phone_book_settings(*args, **kwargs) -> EmployeePhoneBookSettings:
    return EmployeePhoneBookSettings(*args, **kwargs)
