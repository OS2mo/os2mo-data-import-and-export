import asyncio

from mox_helpers.mox_util import ensure_class_value_helper

from exporters.utils.load_settings import load_settings

settings = load_settings()

mox_base = settings.get("mox.base")

eng_type_titles = [
    {
        "uuid": "7cdd6cb3-987c-7b5a-f4f0-30ad519c7481",
        "titel": "Praktikant - Ej lønnet",
    },
    {
        "uuid": "d6e516b5-8821-4db4-9811-88c1ab996b08",
        "titel": "Stud. - EJ LØN Studerende - Ej lønnet",
    },
    {
        "uuid": "bd2f1d9a-f604-480c-83c8-8f4f17af0a2a",
        "titel": "Revi. - EJ LØN Revision - Ej lønnet",
    },
    {
        "uuid": "3d0a7ce8-e9c3-b9d3-84de-4d5ebf7403c2",
        "titel": "Priv. - EJ LØN Ansat i privat virksomhed - Ej lønnet",
    },
    {
        "uuid": "4627516b-5f15-431d-b4ba-a8441dc1dd59",
        "titel": "Afkl. - EJ LØN Afklaringsforløb - Ej lønnet",
    },
    {
        "uuid": "ca19bcd7-499c-4859-a5c9-6ba45b27de85",
        "titel": "Jobp. - EJ LØN Jobprøvning - Ej lønnet",
    },
    {
        "uuid": "45e11b0f-3c48-4dec-a15e-5aec1e6ceb9f",
        "titel": "Friv. - EJ LØN Frivillige - Ej lønnet",
    },
]


async def main():
    tasks = [
        ensure_class_value_helper(
            variable="titel",
            new_value=eng_type["titel"],
            uuid=eng_type["uuid"],
            mox_base=mox_base,
        )
        for eng_type in eng_type_titles
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
