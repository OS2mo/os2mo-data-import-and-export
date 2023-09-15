import pickle

import click
from sdclient.responses import Employment

from sdlon.date_utils import format_date


@click.command()
@click.option(
    "--infile",
    default="/tmp/sdlon/diffs.bin"
)
@click.option(
    "--outfile",
    default="/tmp/sdlon/diffs.csv"
)
def main(
        infile: str,
        outfile: str,
):
    with open(infile, "rb") as fp:
        diffs = pickle.load(fp)

    csv_fields = (
        "cpr",
        "EmploymentIdentifier",
        "SD status code",
        "SD unit",
        "MO unit",
        # "SD job function",
        # "MO job function",
        "SD end date",
        "MO end date",
    )

    csv_lines = [",".join(csv_fields) + "\n"]
    for k, v in diffs.items():
        cpr = f"{k[0][:6]}-xxxx"
        emp_id = k[1]
        sd: Employment = v["sd"]
        mo_eng = v["mo"]
        mismatches = v["mismatches"]

        csv_line = [
            cpr,
            emp_id,
            sd.EmploymentStatus.EmploymentStatusCode if sd is not None else "null"
        ]

        if "Unit" in mismatches:
            csv_line.append(
                str(sd.EmploymentDepartment.DepartmentUUIDIdentifier)
                if sd is not None
                else "null"
            )
            csv_line.append(mo_eng["org_unit"]["uuid"] if mo_eng is not None else "null")
        else:
            csv_line.extend(2 * [""])

        # if "Job function" in mismatches:
        #     csv_line.append(sd.Profession.EmploymentName)
        #     csv_line.append(mo_eng["job_function"])
        # else:
        #     csv_line.extend(2 * [""])

        if "End date" in mismatches:
            csv_line.append(format_date(sd.EmploymentStatus.DeactivationDate) if sd is not None else "null")
            csv_line.append(str(mo_eng["validity"]["to"]) if mo_eng is not None else "null")
        else:
            csv_line.extend(2 * [""])

        if not csv_line[3:] == 4 * [""]:
            csv_lines.append(",".join(csv_line) + "\n")
            print(csv_line)

    with open(outfile, "w") as fp:
        fp.writelines(csv_lines)


if __name__ == "__main__":
    main()
