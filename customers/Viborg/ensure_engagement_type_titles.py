import asyncio

from mox_helpers.mox_util import ensure_class_value_helper
from rautils.load_settings import load_settings

settings = load_settings()

mox_base = settings.get("mox.base")

eng_type_titles = [
    ("7cdd6cb3-987c-7b5a-f4f0-30ad519c7481", "Praktikant - Ej lønnet"),
    ("d6e516b5-8821-4db4-9811-88c1ab996b08", "Studerende - Ej lønnet"),
    ("bd2f1d9a-f604-480c-83c8-8f4f17af0a2a", "Revision - Ej lønnet"),
    ("3d0a7ce8-e9c3-b9d3-84de-4d5ebf7403c2", "Ansat i privat virksomhed - Ej lønnet"),
    ("4627516b-5f15-431d-b4ba-a8441dc1dd59", "Afklaringsforløb - Ej lønnet"),
    ("ca19bcd7-499c-4859-a5c9-6ba45b27de85", "Jobprøvning - Ej lønnet"),
    ("45e11b0f-3c48-4dec-a15e-5aec1e6ceb9f", "Frivillige - Ej lønnet"),
    ("79d50510-4351-272c-e293-4c0d7a50f5bc", "Praktikant - Ej lønnet"),
    ("f3e856ef-21ff-4da6-b820-473d8c159e33", "Studerende - Ej lønnet"),
    ("db1a515f-12ed-4e6e-9410-32a15e4a59af", "Revision - Ej lønnet"),
    ("764b19db-8cb4-8303-e32e-6fc7360b42a5", "Ansat i privat virksomhed - Ej lønnet"),
    ("f75e8c49-c5db-4340-ba75-c34d4b08c395", "Afklaringsforløb - Ej lønnet"),
    ("8ad75c5f-495c-4c93-83c1-13a23a142415", "Jobprøvning - Ej lønnet"),
    ("f6371a1c-c9e4-48a8-87d6-22986ae03acb", "Frivillige - Ej lønnet"),
]


async def main():
    tasks = [
        ensure_class_value_helper(
            variable="titel",
            new_value=title,
            uuid=uuid,
            mox_base=mox_base,
        )
        for uuid, title in eng_type_titles
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
