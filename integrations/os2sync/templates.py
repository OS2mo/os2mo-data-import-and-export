#
# Copyright (c) 2021, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Any, Dict, Optional

from jinja2 import Environment
from jinja2 import StrictUndefined
from jinja2 import Template
from jinja2.exceptions import TemplateSyntaxError

from integrations.os2sync.config import settings as _settings


class FieldTemplateSyntaxError(Exception):
    pass


class FieldTemplateRenderError(Exception):
    pass


class FieldRenderer:
    """Render Jinja templates defined in app settings"""

    def __init__(self, config: Dict[str, str]):
        """Prepare the field renderer, parsing all Jinja templates defined in
        `config`.

        :param config: dictionary, usually `settings["OS2SYNC_TEMPLATES"]`
        """

        # Configure Jinja environment to raise exception on unknown variables
        self._env = Environment(undefined=StrictUndefined)

        def _load_template(key, source):
            try:
                template = self._env.from_string(source)
            except TemplateSyntaxError as e:
                raise FieldTemplateSyntaxError(
                    "syntax error in template %r (source=%r)" % (key, source)
                ) from e
            return template

        # Instantiate all Jinja templates found in config, and map them to
        # their config key
        self._template_fields: Dict[str, Template] = {
            key: _load_template(key, source) for key, source in config.items()
        }

    def render(self, key: str, context: Dict[str, Any], fallback: Any = None) -> str:
        """Render a field template given by `key` using `context`.

        :param key: config key specifying the template to render
        :param context: dictionary used to render template
        :param fallback: value used as fallback in case no template matches `key`
        :raises FieldTemplateRenderError: if template rendering fails
        """

        template = self._template_fields.get(key)

        if not template:
            return fallback

        try:
            return template.render(**context)
        except Exception as e:
            raise FieldTemplateRenderError(
                "could not render template %r (context=%r)" %
                (key, context)
            ) from e


class Entity:
    """Base class for modelling entities defined in the OS2Sync REST API"""

    def __init__(
        self,
        context: Dict[str, Any],
        settings: Optional[Dict[str, str]] = None,
    ):
        """Configure field renderer for this entity.

        :param context: template rendering context, usually a MO JSON response
        :param settings: app settings, defaults to `os2sync.config.settings`
        """

        self.context = context
        self.settings = settings or _settings
        self.field_renderer = FieldRenderer(
            self.settings.get("OS2SYNC_TEMPLATES", {})
        )

    def to_json(self) -> Dict[str, Any]:
        """Return a dictionary suitable for inclusion in a JSON payload."""

        raise NotImplementedError("must be implemented by subclass")


class Person(Entity):
    """Models a `Person` entity in the OS2Sync REST API"""

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.field_renderer.render(
                "person.name", self.context, fallback=self.context["name"],
            ),
            "Cpr": (
                self.context["cpr_no"]
                if self.settings["OS2SYNC_XFER_CPR"] else None
            ),
        }
