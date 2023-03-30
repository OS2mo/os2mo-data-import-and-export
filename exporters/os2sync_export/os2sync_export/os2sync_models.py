from typing import Optional
from typing import Set
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from pydantic import Extra


class OrgUnit(BaseModel):
    """API model for os2sync

    https://www.os2sync.dk/downloads/API%20Documentation.pdf

    public string ShortKey;
    public string Name;
    public string ParentOrgUnitUuid;
    public string PayoutUnitUuid;
    public string ManagerUuid;
    public DateTime Timestamp;
    public string PhoneNumber;
    public string Email;
    public string Location;
    public string LOSShortName;
    public string LOSId;
    public string DtrId;
    public string ContactOpenHours;
    public string EmailRemarks;
    public string Contact;
    public string PostReturn;
    public string PhoneOpenHours;
    public string Ean;
    public string Url;
    public string Landline;
    public string Post;
    public string FOA;
    public string PNR;
    public string SOR;
    public OrgUnitType Type;
    public List<string> Tasks;
    public List<string> ItSystems;
    public List<string> ContactForTasks;
    """

    class Config:
        extra = Extra.ignore

    def json(self):
        return jsonable_encoder(self.dict())

    Uuid: UUID
    ShortKey: Optional[str] = None
    Name: Optional[str]
    ParentOrgUnitUuid: Optional[UUID]
    PayoutUnitUuid: Optional[UUID] = None
    ManagerUuid: Optional[UUID] = None
    PhoneNumber: Optional[str] = None
    Email: Optional[str] = None
    Location: Optional[str] = None
    LOSShortName: Optional[str] = None
    LOSId: Optional[str] = None
    DtrId: Optional[str] = None
    ContactOpenHours: Optional[str] = None
    EmailRemarks: Optional[str] = None
    Contact: Optional[str] = None
    PostReturn: Optional[str] = None
    PhoneOpenHours: Optional[str] = None
    Ean: Optional[str] = None
    Url: Optional[str] = None
    Landline: Optional[str] = None
    Post: Optional[str] = None
    FOA: Optional[str] = None
    PNR: Optional[str] = None
    SOR: Optional[str] = None
    Tasks: Set[UUID] = set()
    ItSystems: Set[UUID] = set()
    ContactForTasks: Set[UUID] = set()
    ContactPlaces: Set[UUID] = set()
