#
# Copyright (c) 2021, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
from typing import Any, Dict, Optional
from uuid import UUID

from jinja2 import Environment, StrictUndefined, Template
from jinja2.exceptions import TemplateSyntaxError

from integrations.os2sync.config import loggername as _loggername
from integrations.os2sync.config import get_os2sync_settings

logger = logging.getLogger(_loggername)


class FieldTemplateSyntaxError(Exception):
    pass


class FieldTemplateRenderError(Exception):
    pass


class FieldRenderer:
    """Render Jinja templates defined in app settings"""

    def __init__(self, config: Dict[str, str]):
        """Prepare the field renderer, parsing all Jinja templates defined in
        `config`.

        :param config: dictionary, usually `settings.os2sync_templates`
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
        settings: Dict[str, str],
    ):
        """Configure field renderer for this entity.

        :param context: template rendering context, usually a MO JSON response
        :param settings: app settings, defaults to `os2sync.config.settings`
        """

        self.context = context
        self.settings = settings
        self.field_renderer = FieldRenderer(
            self.settings.os2sync_templates
        )

    def to_json(self) -> Dict[str, Any]:
        """Return a dictionary suitable for inclusion in a JSON payload."""

        raise NotImplementedError("must be implemented by subclass")


class User(Entity):
    """Models a `User` entity in the OS2Sync REST API"""

    def __init__(
        self,
        context: Dict[str, Any],
        settings: Optional[Dict[str, str]] = None,
    ):
        super().__init__(context, settings=settings)
        assert isinstance(context["uuid"], (UUID, str))
        assert isinstance(context["person"], Person)
        assert isinstance(context["candidate_user_id"], (str, type(None)))
        self.context.setdefault("user_key", context["person"].context["user_key"])

    def to_json(self) -> Dict[str, Any]:
        if self.context["candidate_user_id"] is not None:
            # If an AD BVN is available, always use that
            user_id = self.context["candidate_user_id"]
        else:
            # Otherwise, use the "person.user_id" template if available,
            # falling back to the MO user UUID if not.
            user_id = self.field_renderer.render(
                "person.user_id", self.context, fallback=self.context["uuid"],
            )

        person = self.context["person"]

        return {
            "Uuid": self.context["uuid"],
            "UserId": user_id,
            "Person": person.to_json(),
            "Positions": [],
        }


class Person(Entity):
    """Models a `Person` entity in the OS2Sync REST API"""

    def to_json(self) -> Dict[str, Any]:
        if self.settings.os2sync_xfer_cpr:
            cpr = self.context.get("cpr_no")
            if not cpr:
                logger.warning("no 'cpr_no' for user %r", self.context["uuid"])
            else:
                logger.debug("transferring CPR for user %r", self.context["uuid"])
        else:
            cpr = None
            logger.debug("not configured to transfer CPR")

        return {
            "Name": self.field_renderer.render(
                "person.name", self.context, fallback=self.context["name"],
            ),
            "Cpr": cpr,
        }
