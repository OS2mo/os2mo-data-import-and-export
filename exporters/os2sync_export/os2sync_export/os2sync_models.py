from typing import List
from typing import Optional
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field


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
        allow_population_by_field_name = True
        extra = Extra.ignore

    def json(self):
        return jsonable_encoder(self.dict())

    Uuid: UUID = Field(..., alias="uuid")
    ShortKey: Optional[str] = Field(None, alias="shortKey")
    Name: Optional[str] = Field(..., alias="name")
    ParentOrgUnitUuid: Optional[UUID] = Field(..., alias="parentOrgUnitUuid")
    PayoutUnitUuid: Optional[UUID] = Field(None, alias="payoutUnitUuid")
    ManagerUuid: Optional[UUID] = Field(None, alias="managerUuid")
    # Timestamp: Optional[str] = Field(None, alias="timestamp")
    PhoneNumber: Optional[str] = Field(None, alias="phoneNumber")
    Email: Optional[str] = Field(None, alias="email")
    Location: Optional[str] = Field(None, alias="location")
    LOSShortName: Optional[str] = Field(None, alias="losShortName")
    LOSId: Optional[str] = Field(None, alias="losId")
    DtrId: Optional[str] = Field(None, alias="dtrId")
    ContactOpenHours: Optional[str] = Field(None, alias="contactOpenHours")
    EmailRemarks: Optional[str] = Field(None, alias="emailRemarks")
    Contact: Optional[str] = Field(None, alias="contact")
    PostReturn: Optional[str] = Field(None, alias="postReturn")
    PhoneOpenHours: Optional[str] = Field(None, alias="phoneOpenHours")
    Ean: Optional[str] = Field(None, alias="ean")
    Url: Optional[str] = Field(None, alias="url")
    Landline: Optional[str] = Field(None, alias="landline")
    Post: Optional[str] = Field(None, alias="post")
    FOA: Optional[str] = Field(None, alias="foa")
    PNR: Optional[str] = Field(None, alias="pnr")
    SOR: Optional[str] = Field(None, alias="sor")
    # Type: Optional[str] = Field(None, alias="type")
    Tasks: List[UUID] = Field([], alias="tasks")
    ItSystems: List[UUID] = Field([], alias="itSystems")
    ContactForTasks: List[UUID] = Field([], alias="contactForTasks")
    ContactPlaces: List[UUID] = Field([], alias="contactPlaces")
