"""
Schemata

This module provides declarative typed specifications for data frames and also
graphs. In particular:

  - `PARTIAL_SCHEMA`, `BASE_SCHEMA`, and `SCHEMA` are all schemas for the
    original transparency database. As the name already implies,
    `PARTIAL_SCHEMA` covers only some fields. Both `PARTIAL_SCHEMA` and
    `BASE_SCHEMA` are imprecise and only used temporarily, before fixing a data
    frame's contents to adhere to `SCHEMA`.
  - `STATISTICS_SCHEMA` is the comparably simpler schema for summary statistics,
    which are collected in a non-tidy, mostly long data frame. The reason for
    the "mostly" qualifier is that the data frame comprises four columns with
    integer values, `count`, `min`, `mean`, and `max`, instead of a single one
    because each column aggregates differently.
  - `TRANSFORMS` provides a declarative specification for deriving the summary
    statistics from the original database table. It comprises five
    non-parametric and two parametric transforms. One of the latter two is used
    for defining virtual fields that were not part of the original schema.
  - `MetricDeclaration` instances serve dual purposes. They define precise
    enumeration types for the transparency database schema. They also include
    enough information for a more humane presentation of enumeration constants
    in graphs.
"""
from collections.abc import Sequence
from dataclasses import dataclass
import datetime as dt
import enum
from types import GenericAlias, MappingProxyType
from typing import Any, get_args, get_origin, Literal

import polars as pl

from .color import (
    BLUE, BROWN, CYAN, DARK_PURPLE, GRAY, GREEN, LIGHT_BLUE, ORANGE, PINK,
    PURPLE, RED, YELLOW_GREEN,
)


# ======================================================================================
# Language and Country Codes


class ContentLanguage(enum.Enum):
    AA = "Afar"
    AB = "Abkhazian"
    AE = "Avestan"
    AF = "Afrikaans"
    AK = "Akan"
    AM = "Amharic"
    AN = "Aragonese"
    AR = "Arabic"
    AS = "Assamese"
    AV = "Avaric"
    AY = "Aymara"
    AZ = "Azerbaijani"
    BA = "Bashkir"
    BE = "Belarusian"
    BG = "Bulgarian"
    BI = "Bislama"
    BM = "Bambara"
    BN = "Bengali"
    BO = "Tibetan"
    BR = "Breton"
    BS = "Bosnian"
    CA = "Catalan"
    CE = "Chechen"
    CH = "Chamorro"
    CO = "Corsican"
    CR = "Cree"
    CS = "Czech"
    CU = "Church Slavonic"
    CV = "Chuvash"
    CY = "Welsh"
    DA = "Danish"
    DE = "German"
    DV = "Divehi"
    DZ = "Dzongkha"
    EE = "Ewe"
    EL = "Greek"
    EN = "English"
    EO = "Esperanto"
    ES = "Spanish"
    ET = "Estonian"
    EU = "Basque"
    FA = "Persian"
    FF = "Fulah"
    FI = "Finnish"
    FJ = "Fijian"
    FO = "Faroese"
    FR = "French"
    FY = "Western Frisian"
    GA = "Irish"
    GD = "Gaelic"
    GL = "Galician"
    GN = "Guarani"
    GU = "Gujarati"
    GV = "Manx"
    HA = "Hausa"
    HE = "Hebrew"
    HI = "Hindi"
    HO = "Hiri Motu"
    HR = "Croatian"
    HT = "Haitian"
    HU = "Hungarian"
    HY = "Armenian"
    HZ = "Herero"
    IA = "Interlingua"
    ID = "Indonesian"
    IE = "Interlingue"
    IG = "Igbo"
    II = "Sichuan Yi"
    IK = "Inupiaq"
    IO = "Ido"
    IS = "Icelandic"
    IT = "Italian"
    IU = "Inuktitut"
    JA = "Japanese"
    JV = "Javanese"
    KA = "Georgian"
    KG = "Kongo"
    KI = "Kikuyu"
    KJ = "Kuanyama"
    KK = "Kazakh"
    KL = "Kalaallisut"
    KM = "Central Khmer"
    KN = "Kannada"
    KO = "Korean"
    KR = "Kanuri"
    KS = "Kashmiri"
    KU = "Kurdish"
    KV = "Komi"
    KW = "Cornish"
    KY = "Kyrgyz"
    LA = "Latin"
    LB = "Luxembourgish"
    LG = "Ganda"
    LI = "Limburgan"
    LN = "Lingala"
    LO = "Lao"
    LT = "Lithuanian"
    LU = "Luba-Katanga"
    LV = "Latvian"
    MG = "Malagasy"
    MH = "Marshallese"
    MI = "Maori"
    MK = "Macedonian"
    ML = "Malayalam"
    MN = "Mongolian"
    MR = "Marathi"
    MS = "Malay"
    MT = "Maltese"
    MY = "Burmese"
    NA = "Nauru"
    NB = "Norwegian Bokmål"
    ND = "North Ndebele"
    NE = "Nepali"
    NG = "Ndonga"
    NL = "Dutch"
    NN = "Norwegian Nynorsk"
    NO = "Norwegian"
    NR = "South Ndebele"
    NV = "Navajo"
    NY = "Chichewa"
    OC = "Occitan"
    OJ = "Ojibwa"
    OM = "Oromo"
    OR = "Oriya"
    OS = "Ossetian"
    PA = "Punjabi"
    PI = "Pali"
    PL = "Polish"
    PS = "Pashto"
    PT = "Portuguese"
    QU = "Quechua"
    RM = "Romansh"
    RN = "Rundi"
    RO = "Romanian"
    RU = "Russian"
    RW = "Kinyarwanda"
    SA = "Sanskrit"
    SC = "Sardinian"
    SD = "Sindhi"
    SE = "Northern Sami"
    SG = "Sango"
    SI = "Sinhala"
    SK = "Slovak"
    SL = "Slovenian"
    SM = "Samoan"
    SO = "Somali"
    SN = "Shona"
    SQ = "Albanian"
    SR = "Serbian"
    SS = "Swati"
    ST = "Southern Sotho"
    SU = "Sundanese"
    SV = "Swedish"
    SW = "Swahili"
    TA = "Tamil"
    TE = "Telugu"
    TG = "Tajik"
    TH = "Thai"
    TI = "Tigrinya"
    TK = "Turkmen"
    TL = "Tagalog"
    TN = "Tswana"
    TO = "Tsonga"
    TR = "Turkish"
    TT = "Tatar"
    TW = "Twi"
    TY = "Tahitian"
    UG = "Uighur"
    UK = "Ukrainian"
    UR = "Urdu"
    UZ = "Uzbek"
    VE = "Venda"
    VI = "Vietnamese"
    VO = "Volapük"
    WA = "Walloon"
    WO = "Wolof"
    XH = "Xhosa"
    YI = "Yiddish"
    YO = "Yoruba"
    ZA = "Zhuang"
    ZH = "Chinese"
    ZU = "Zulu"


class TerritorialScope(enum.Enum):
    EU = "EU"
    EEA = "EEA"
    EEA_no_IS = "EEA_no_IS"
    AT = "Austria"
    BE = "Belgium"
    BG = "Bulgaria"
    CY = "Cyprus"
    CZ = "Czechia"
    DE = "Germany"
    DK = "Denmark"
    EE = "Estonia"
    ES = "Spain"
    FI = "Finland"
    FR = "France"
    GR = "Greece"
    HR = "Croatia"
    HU = "Hungary"
    IE = "Ireland"
    IS = "Iceland"
    IT = "Italy"
    LI = "Liechtenstein"
    LT = "Lithuania"
    LU = "Luxembourg"
    LV = "Latvia"
    MT = "Malta"
    NL = "Netherlands"
    NO = "Norway"
    PL = "Poland"
    PT = "Portugal"
    RO = "Romania"
    SE = "Sweden"
    SI = "Slovenia"
    SK = "Slovakia"


class TerritorialAlias(enum.StrEnum):
    EU = (
    '["AT","BE","BG","CY","CZ","DE","DK","EE","ES","FI","FR","GR","HR","HU","IE",'
    '"IT","LT","LU","LV","MT","NL","PL","PT","RO","SE","SI","SK"]'
    )
    EEA = (
        '["AT","BE","BG","CY","CZ","DE","DK","EE","ES","FI","FR","GR","HR","HU","IE",'
        '"IS","IT","LI","LT","LU","LV","MT","NL","NO","PL","PT","RO","SE","SI","SK"]'
    )
    EEA_no_IS = (
        '["AT","BE","BG","CY","CZ","DE","DK","EE","ES","FI","FR","GR","HR","HU","IE",'
        '"IT","LI","LT","LU","LV","MT","NL","NO","PL","PT","RO","SE","SI","SK"]'
    )


# ======================================================================================


type VariantNamesAndColors = dict[None |str, tuple[str, str]]

@dataclass(frozen=True, slots=True)
class MetricDeclaration:
    """A declarative specification of how to visually present a variant."""

    field: str | Sequence[str]
    label: str
    selector: Literal["column", "entity", "variant"]
    quantity: Literal["count", "min", "mean", "max"]
    quant_label: str
    variants: VariantNamesAndColors

    def __init__(
        self,
        field: str | Sequence[str],
        label: str,
        variants: VariantNamesAndColors,
        *,
        selector: Literal["column", "entity", "variant"] = "variant",
        quantity: Literal["count", "min", "mean", "max"] = "count",
        quant_label: str = "Statements of Reasons",
    ) -> None:
        object.__setattr__(self, "field", field)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "variants", MappingProxyType(variants))
        object.__setattr__(self, "selector", selector)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "quant_label", quant_label)

    def has_variants(self) -> bool:
        return 0 < len(self.variants)

    def has_null_variant(self) -> bool:
        return None in self.variants

    def variant_names(self) -> list[str]:
        return [k for k in self.variants.keys() if k is not None]

    def enum(self) -> pl.Enum:
        return pl.Enum(self.variant_names())

    def replacements(self) -> dict[None | str, str]:
        return {k: v[0] for k, v in self.variants.items()}

    def variant_labels(self) -> list[str]:
        return [v[0] for v in self.variants.values()]

    def variant_colors(self) -> list[str]:
        return [v[1] for v in self.variants.values()]

    def groupings(self) -> list[pl.Expr]:
        groupings = [pl.col("column")]
        if self.selector != "column":
            groupings.append(pl.col("entity"))
        if self.selector == "variant":
            groupings.append(pl.col("variant"))
        return groupings


AccountType = MetricDeclaration("account_type", "Account Types", {
    "ACCOUNT_TYPE_BUSINESS": ("Business", ORANGE),
    "ACCOUNT_TYPE_PRIVATE": ("Individual", BLUE),
    None: ("—none—", RED),
})


AutomatedDecision = MetricDeclaration("automated_decision", "Automated Decisions", {
    "AUTOMATED_DECISION_FULLY": ("Fully Automated", CYAN),
    "AUTOMATED_DECISION_PARTIALLY": ("Partially Automated", BLUE),
    "AUTOMATED_DECISION_NOT_AUTOMATED": ("Not Automated", GREEN),
    None: ("—none—", RED),
})


AutomatedDetection = MetricDeclaration("automated_detection", "Automated Detection", {
    "Yes": ("Automated", LIGHT_BLUE),
    "No": ("Not Automated", PURPLE),
    None: ("—none—", RED),
})


ContentType = MetricDeclaration("content_type", "Content Types", {
    "CONTENT_TYPE_APP": ("App", CYAN),
    "CONTENT_TYPE_AUDIO": ("Audio", GREEN),
    "CONTENT_TYPE_IMAGE": ("Image", BLUE),
    "CONTENT_TYPE_PRODUCT": ("Product", RED),
    "CONTENT_TYPE_SYNTHETIC_MEDIA": ("Synthetic Media", PINK),
    "CONTENT_TYPE_TEXT": ("Text", ORANGE),
    "CONTENT_TYPE_VIDEO": ("Video", PURPLE),
    "CONTENT_TYPE_OTHER": ("Other", LIGHT_BLUE),
    None: ("—none—", GRAY),
})


DecisionAccount = MetricDeclaration("decision_account", "Account Decisions", {
    "DECISION_ACCOUNT_SUSPENDED": ("Suspended", ORANGE),
    "DECISION_ACCOUNT_TERMINATED": ("Terminated", RED),
    None: ("—none—", GRAY),
})


DecisionGround = MetricDeclaration("decision_ground", "Decision Grounds", {
    "DECISION_GROUND_ILLEGAL_CONTENT": ("Illegal", ORANGE),
    "DECISION_GROUND_INCOMPATIBLE_CONTENT": ("Incompatible", GREEN),
})


# Combine decision_ground and incompatible_content_illegal
DecisionGroundAndLegality = MetricDeclaration(
    ["decision_ground", "incompatible_content_illegal"],
    "Decision Grounds",
    DecisionGround.variants | {"Yes": ("Incompatible & Illegal", RED)}
)


DecisionMonetary = MetricDeclaration("decision_monetary", "Monetary Decisions", {
   "DECISION_MONETARY_SUSPENSION": ("Suspended", ORANGE),
   "DECISION_MONETARY_TERMINATION": ("Terminated", RED),
   "DECISION_MONETARY_OTHER": ("Other", PINK),
   None: ("—none—", GRAY),
})


DecisionProvision = MetricDeclaration("decision_provision", "Service Provision Decisions", {
    "DECISION_PROVISION_PARTIAL_SUSPENSION": ("Partially Suspended", LIGHT_BLUE),
    "DECISION_PROVISION_TOTAL_SUSPENSION": ("Suspended", BLUE),
    "DECISION_PROVISION_PARTIAL_TERMINATION": ("Partially Terminated", ORANGE),
    "DECISION_PROVISION_TOTAL_TERMINATION": ("Terminated", RED),
    None: ("—none—", GRAY),
})


DecisionType = MetricDeclaration("decision_type", "Decision Types", {
    "vis": ("Visibility", BLUE),
    "mon": ("Monetary", YELLOW_GREEN),
    "vis_mon": ("Visibility & Monetary", DARK_PURPLE),
    "pro": ("Provision", LIGHT_BLUE),
    "vis_pro": ("Visibility & Provision", ORANGE),
    "mon_pro": ("Monetary & Provision", GREEN),
    "vis_mon_pro": ("Visibility, Monetary, Provision", CYAN),
    "acc": ("Account", PURPLE),
    "vis_acc": ("Visibility & Account", PINK),
    "mon_acc": ("Monetary & Account", GREEN),
    "vis_mon_acc": ("Visibility, Monetary, Account", CYAN),
    "pro_acc": ("Provision & Account", GREEN),
    "vis_pro_acc": ("Visibility, Provision, Account", RED),
    "mon_pro_acc": ("Monetary, Provision, Account", CYAN),
    "vis_mon_pro_acc": ("Visibility, Monetary, Provision, Account", GREEN),
    None: ("—none—", GRAY),
}, selector="entity")


DecisionVisibility = MetricDeclaration("decision_visibility", "Visibility Decisions", {
    "DECISION_VISIBILITY_CONTENT_REMOVED": ("Removed", LIGHT_BLUE),
    "DECISION_VISIBILITY_CONTENT_DISABLED": ("Disabled", RED),
    "DECISION_VISIBILITY_CONTENT_DEMOTED": ("Demoted", ORANGE),
    "DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED": ("Age-Restricted", GREEN),
    "DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED": ("Interaction Restricted", PURPLE),
    "DECISION_VISIBILITY_CONTENT_LABELLED": ("Labelled", PINK),
    "DECISION_VISIBILITY_OTHER": ("Other", BLUE),
    None: ("—none—", GRAY),
})


InformationSource = MetricDeclaration("source_type", "Information Sources", {
    "SOURCE_ARTICLE_16": ("Article 16", LIGHT_BLUE),
    "SOURCE_TRUSTED_FLAGGER": ("Trusted Flagger", BLUE),
    "SOURCE_TYPE_OTHER_NOTIFICATION": ("Other Notification", ORANGE),
    "SOURCE_VOLUNTARY": ("Voluntary", GREEN),
    None: ("—none—", GRAY),
})


Keyword = (
    # --- Animal welfare
    "KEYWORD_ANIMAL_HARM",
    "KEYWORD_UNLAWFUL_SALE_ANIMALS",

    # --- Consumer information (v2)
    "KEYWORD_HIDDEN_ADVERTISEMENT",
    "KEYWORD_INSUFFICIENT_INFORMATION_ON_TRADERS",
    "KEYWORD_MISLEADING_INFO_CONSUMER_RIGHTS",
    "KEYWORD_MISLEADING_INFO_GOODS_SERVICES",
    "KEYWORD_NONCOMPLIANCE_PRICING",

    # --- Cyber violence (v2)
    "KEYWORD_CYBER_BULLYING_INTIMIDATION",
    "KEYWORD_CYBER_HARASSMENT",
    "KEYWORD_CYBER_INCITEMENT",
    "KEYWORD_CYBER_STALKING",
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING",
    "KEYWORD_NON_CONSENSUAL_MATERIAL_DEEPFAKE",

    # --- Cyber violence against women (v2)
    "KEYWORD_BULLYING_AGAINST_GIRLS",
    "KEYWORD_CYBER_HARASSMENT_AGAINST_WOMEN",
    "KEYWORD_CYBER_STALKING_AGAINST_WOMEN",
    "KEYWORD_FEMALE_GENDERED_DISINFORMATION",
    "KEYWORD_INCITEMENT_AGAINST_WOMEN",
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING_AGAINST_WOMEN",
    "KEYWORD_NON_CONSENSUAL_MATERIAL_DEEPFAKE_AGAINST_WOMEN",

    # --- Data protection and privacy violations
    "KEYWORD_BIOMETRIC_DATA_BREACH",
    "KEYWORD_MISSING_PROCESSING_GROUND",
    "KEYWORD_RIGHT_TO_BE_FORGOTTEN",
    "KEYWORD_DATA_FALSIFICATION",

    # --- Illegal or harmful speech
    "KEYWORD_DEFAMATION",
    "KEYWORD_DISCRIMINATION",
    "KEYWORD_HATE_SPEECH",

    # --- Intellectual property infringements
    "KEYWORD_COPYRIGHT_INFRINGEMENT",
    "KEYWORD_DESIGN_INFRINGEMENT",
    "KEYWORD_GEOGRAPHIC_INDICATIONS_INFRINGEMENT",
    "KEYWORD_PATENT_INFRINGEMENT",
    "KEYWORD_TRADE_SECRET_INFRINGEMENT",
    "KEYWORD_TRADEMARK_INFRINGEMENT",

    # --- Negative effects on civic discourse or elections
    "KEYWORD_DISINFORMATION",
    "KEYWORD_MISINFORMATION",
    "KEYWORD_MISINFORMATION_DISINFORMATION",
    "KEYWORD_VIOLATION_EU_LAW",
    "KEYWORD_VIOLATION_NATIONAL_LAW",
    "KEYWORD_FOREIGN_INFORMATION_MANIPULATION",

    # --- Non-consensual behavior
    "KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE",
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION",
    "KEYWORD_STALKING",

    # --- Pornography or sexualized content
    "KEYWORD_ADULT_SEXUAL_MATERIAL",
    "KEYWORD_IMAGE_BASED_SEXUAL_ABUSE",

    # --- Protection of minors
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL_DEEPFAKE",
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS",
    "KEYWORD_UNSAFE_CHALLENGES",

    # --- Risk for public security
    "KEYWORD_ILLEGAL_ORGANIZATIONS",
    "KEYWORD_RISK_ENVIRONMENTAL_DAMAGE",
    "KEYWORD_RISK_PUBLIC_HEALTH",
    "KEYWORD_TERRORIST_CONTENT",

    # --- Scams and/or fraud
    "KEYWORD_INAUTHENTIC_ACCOUNTS",
    "KEYWORD_INAUTHENTIC_LISTINGS",
    "KEYWORD_INAUTHENTIC_USER_REVIEWS",
    "KEYWORD_IMPERSONATION_ACCOUNT_HIJACKING",
    "KEYWORD_PHISHING",
    "KEYWORD_PYRAMID_SCHEMES",

    # --- Self-harm
    "KEYWORD_CONTENT_PROMOTING_EATING_DISORDERS",
    "KEYWORD_SELF_MUTILATION",
    "KEYWORD_SUICIDE",

    # --- Scope of platform service
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS",
    "KEYWORD_GEOGRAPHICAL_REQUIREMENTS",
    "KEYWORD_GOODS_SERVICES_NOT_PERMITTED",
    "KEYWORD_LANGUAGE_REQUIREMENTS",
    "KEYWORD_NUDITY",

    # --- Unsafe and/or illegal products
    "KEYWORD_INSUFFICIENT_INFORMATION_TRADERS",
    "KEYWORD_PROHIBITED_PRODUCTS",
    "KEYWORD_UNSAFE_PRODUCTS",
    "KEYWORD_REGULATED_GOODS_SERVICES",
    "KEYWORD_DANGEROUS_TOYS",

    # --- Violence
    "KEYWORD_COORDINATED_HARM",
    "KEYWORD_GENDER_BASED_VIOLENCE",
    "KEYWORD_HUMAN_EXPLOITATION",
    "KEYWORD_HUMAN_TRAFFICKING",
    "KEYWORD_INCITEMENT_VIOLENCE_HATRED",
    "KEYWORD_TRAFFICKING_WOMEN_GIRLS",

    # --- Other
    "KEYWORD_OTHER",
)


# Cover all keywords that are utilized in practice
KeywordsMinorProtection = MetricDeclaration("category_specification", "Keywords", {
    "KEYWORD_ADULT_SEXUAL_MATERIAL": ("Adult Sexual Material", GREEN),
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS": ("Age-Restricted", PURPLE),
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL": ("CSAM", LIGHT_BLUE),
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL_DEEPFAKE": ("Deepfake", PINK),
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS": ("Grooming", RED),
    "KEYWORD_HATE_SPEECH": ("Hate Speech", CYAN),
    "KEYWORD_HUMAN_TRAFFICKING": ("Trafficking", ORANGE),
    "KEYWORD_NUDITY": ("Nudity", DARK_PURPLE),
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION": ("Bullying", GRAY),
    "KEYWORD_OTHER": ("Other", BLUE),
    "KEYWORD_REGULATED_GOODS_SERVICES": ("Regulated Goods/Services", BROWN),
    "KEYWORD_UNSAFE_CHALLENGES": ("Unsafe Challenges", YELLOW_GREEN),
}, quant_label="SoRs with Keyword")


from ._platform import (
    CanonicalPlatformNames as CanonicalPlatformNames,
    PlatformNames as PlatformNames,
)


ProcessingDelay = MetricDeclaration(
    ["moderation_delay", "disclosure_delay"],
    "Delays",
    {
        "moderation_delay": ("Moderation", LIGHT_BLUE),
        "disclosure_delay": ("Disclosure", RED),
        None: ("—none—", GRAY),
    },
    selector="column",
    quantity="mean",
    quant_label="Days",
)


# See
# https://transparency.dsa.ec.europa.eu/page/additional-explanation-for-statement-attributes
# for two-level classification for types of violative activity.

StatementCategory = (
    "STATEMENT_CATEGORY_ANIMAL_WELFARE",
    "STATEMENT_CATEGORY_CONSUMER_INFORMATION",
    "STATEMENT_CATEGORY_CYBER_VIOLENCE",
    "STATEMENT_CATEGORY_CYBER_VIOLENCE_AGAINST_WOMEN",
    "STATEMENT_CATEGORY_DATA_PROTECTION_AND_PRIVACY_VIOLATIONS",
    "STATEMENT_CATEGORY_ILLEGAL_OR_HARMFUL_SPEECH",
    "STATEMENT_CATEGORY_INTELLECTUAL_PROPERTY_INFRINGEMENTS",
    "STATEMENT_CATEGORY_NEGATIVE_EFFECTS_ON_CIVIC_DISCOURSE_OR_ELECTIONS",
    "STATEMENT_CATEGORY_NON_CONSENSUAL_BEHAVIOUR",
    "STATEMENT_CATEGORY_NOT_SPECIFIED_NOTICE",
    "STATEMENT_CATEGORY_OTHER_VIOLATION_TC",
    "STATEMENT_CATEGORY_PORNOGRAPHY_OR_SEXUALIZED_CONTENT",
    "STATEMENT_CATEGORY_PROTECTION_OF_MINORS",
    "STATEMENT_CATEGORY_RISK_FOR_PUBLIC_SECURITY",
    "STATEMENT_CATEGORY_SCAMS_AND_FRAUD",
    "STATEMENT_CATEGORY_SELF_HARM",
    "STATEMENT_CATEGORY_SCOPE_OF_PLATFORM_SERVICE",
    "STATEMENT_CATEGORY_UNSAFE_AND_ILLEGAL_PRODUCTS",
    "STATEMENT_CATEGORY_UNSAFE_AND_PROHIBITED_PRODUCTS",
    "STATEMENT_CATEGORY_VIOLENCE",
)


StatementCount = MetricDeclaration(
    "rows",
    "Statement Counts",
    {},
    selector="column",
)


YesNo = (
    "Yes",
    "No",
)


# ======================================================================================
# Schemata


FIELDS = MappingProxyType({
    "uuid": str,

    "decision_visibility": list[DecisionVisibility],
    "decision_visibility_other": str,
    "end_date_visibility_restriction": dt.datetime,

    "decision_monetary": DecisionMonetary,
    "decision_monetary_other": str,
    "end_date_monetary_restriction": dt.datetime,

    "decision_provision": DecisionProvision,
    "end_date_service_restriction": dt.datetime,

    "decision_account": DecisionAccount,
    "end_date_account_restriction": dt.datetime,

    "account_type": AccountType,

    "decision_ground": DecisionGround,
    "decision_ground_reference_url": str,

    "illegal_content_legal_ground": str,
    "illegal_content_explanation": str,

    "incompatible_content_ground": str,
    "incompatible_content_explanation": str,
    "incompatible_content_illegal": YesNo,

    "category": StatementCategory,
    "category_addition": list[StatementCategory],
    "category_specification": list[Keyword],
    "category_specification_other": str,

    "content_type": list[ContentType],
    "content_type_other": str,
    "content_language": tuple(v.name for v in ContentLanguage),
    "content_date": dt.datetime,

    "territorial_scope": list[tuple(v.name for v in TerritorialScope)],
    "application_date": dt.datetime,
    "decision_facts": str,

    "source_type": InformationSource,
    "source_identity": str,
    "automated_detection": YesNo,
    "automated_decision": AutomatedDecision,

    "platform_name": str,
    "platform_uid": str,

    "created_at": dt.datetime,
    "released_on": dt.date,
})


def polarize(
    ptype: GenericAlias | MetricDeclaration | tuple[str, ...] | type
) -> Any:
    """
    Convert a Python type to a Pola.rs type. This function handles int, float,
    str, datetime.date, datetime.datetime, and list[<type>]. It also treats
    tuples of strings as enumerations.
    """
    if ptype is dt.date:
        return pl.Date
    if ptype is dt.datetime:
        return pl.Datetime(time_unit="ms")
    if ptype is int:
        return pl.Int64
    if ptype is float:
        return pl.Float64
    if ptype is str:
        return pl.String
    if isinstance(ptype, tuple) and all(isinstance(v, str) for v in ptype):
        return pl.Enum(ptype)
    if isinstance(ptype, MetricDeclaration):
        return ptype.enum()

    origin = get_origin(ptype)
    args = get_args(ptype)

    if origin is list:
        if 1 == len(args):
            return pl.List(polarize(args[0]))
        if 1 < len(args):
            # list[tuple(...)] inlines the explicit tuple into the args tuple.
            return pl.List(polarize(args))

    raise ValueError(f'cannot convert "{ptype}" with type {type(ptype)}')


def _generate_schemata() -> tuple[pl.Schema, pl.Schema, pl.Schema]:
    partial = {}
    base = {}
    full = {}

    for name, ptype in FIELDS.items():
        dtype = polarize(ptype)
        is_enum = isinstance(dtype, pl.Enum)

        full[name] = dtype
        if name == "released_on":
            continue

        if is_enum and name != "content_language":
            partial[name] = dtype

        if is_enum:
            base[name] = dtype
        else:
            base[name] = pl.String

    return pl.Schema(partial), pl.Schema(base), pl.Schema(full)

PARTIAL_SCHEMA, BASE_SCHEMA, SCHEMA = _generate_schemata()
del _generate_schemata


# ======================================================================================
# Declaration of Statistics Transforms


class TransformType(enum.Enum):
    """Non-parametric transform types."""
    SKIPPED_DATE = enum.auto()
    ROWS = enum.auto()
    VALUE_COUNTS = enum.auto()
    LIST_VALUE_COUNTS = enum.auto()
    DECISION_TYPE = enum.auto()


@dataclass(frozen=True, slots=True)
class DurationTransform:
    """A duration is the difference of two datetimes."""
    start: str
    end: str


@dataclass(frozen=True, slots=True)
class ValueCountsPlusTransform:
    """Value counts for a field as well as in combination with another one."""
    self_is_list: bool
    other_field: str
    other_is_list: bool = False


# The transforms cover all DSA transparency database entries without unconstrained text.
TRANSFORMS = {
    "rows": TransformType.ROWS,
    "decision_type": TransformType.DECISION_TYPE,
    "decision_visibility": ValueCountsPlusTransform(
        self_is_list=True, other_field="end_date_visibility_restriction"
    ),
    "end_date_visibility_restriction": TransformType.SKIPPED_DATE,
    "visibility_restriction_duration": DurationTransform(
        "application_date", "end_date_visibility_restriction"
    ),
    "decision_monetary": TransformType.VALUE_COUNTS,
    "end_date_monetary_restriction": TransformType.SKIPPED_DATE,
    "monetary_restriction_duration": DurationTransform(
        "application_date", "end_date_monetary_restriction"
    ),
    "decision_provision": ValueCountsPlusTransform(
        self_is_list=False, other_field="end_date_service_restriction"),
    "end_date_service_restriction": TransformType.SKIPPED_DATE,
    "service_restriction_duration": DurationTransform(
        "application_date", "end_date_service_restriction"
    ),
    "decision_account": ValueCountsPlusTransform(
        self_is_list=False, other_field="end_date_account_restriction"
    ),
    "end_date_account_restriction": TransformType.SKIPPED_DATE,
    "account_restriction_duration": DurationTransform(
        "application_date", "end_date_account_restriction"
    ),
    "account_type": TransformType.VALUE_COUNTS,
    "decision_ground": TransformType.VALUE_COUNTS,
    "incompatible_content_illegal": TransformType.VALUE_COUNTS,
    "category": TransformType.VALUE_COUNTS,
    "category_addition": TransformType.LIST_VALUE_COUNTS,
    "category_specification": TransformType.LIST_VALUE_COUNTS,
    "content_type": TransformType.LIST_VALUE_COUNTS,
    "content_language": TransformType.VALUE_COUNTS,
    "moderation_delay": DurationTransform("content_date", "application_date"),
    "disclosure_delay": DurationTransform("application_date", "created_at"),
    "source_type": TransformType.VALUE_COUNTS,
    "automated_detection": TransformType.VALUE_COUNTS,
    "automated_decision": TransformType.VALUE_COUNTS,
    "platform_name": ValueCountsPlusTransform(
        self_is_list=False, other_field="category_specification", other_is_list=True
    ),
}

TRANSFORM_COUNT = sum(
    (0 if v is TransformType.SKIPPED_DATE else 1)
    for v in TRANSFORMS.values()
)


# ======================================================================================
# Statistics Schema


ColumnValueType = pl.Enum((
    "start_date",
    "end_date",
    "batch_count",
    "batch_rows",
    "batch_rows_with_keywords",
    "batch_memory",
    "total_rows",
    "total_rows_with_keywords",
    "rows",
    "decision_type",
    "visibility_restriction_duration",
    "monetary_restriction_duration",
    "service_restriction_duration",
    "account_restriction_duration",
    "moderation_delay",
    "disclosure_delay",
    *(c for c in SCHEMA.names())
))


EntityValueType = pl.Enum((
    "is_null",
    "vis",
    "mon",
    "vis_mon",
    "pro",
    "vis_pro",
    "mon_pro",
    "vis_mon_pro",
    "acc",
    "vis_acc",
    "mon_acc",
    "vis_mon_acc",
    "pro_acc",
    "vis_pro_acc",
    "mon_pro_acc",
    "vis_mon_pro_acc",
    "with_end_date",
    "elements",
    "elements_per_row",
    "rows_with_elements",
    "with_category_specification",
))


def _all_variants() -> list[str]:
    """
    Collect *all* known enum variants into a list.

    This function effectively computes the type union of all enum types used by
    the transparency database (while also accounting for overlap between the
    two-letter-codes of ContentLanguage and TerritorialScope). It becomes the
    type of the variant column in the summary statistics.

    The challenge in computing this union is that must include the values of
    platform_name, which go through some degree of churn. Solely relying on
    releases to address this churn is not very nimble. Instead, shantay checks
    transparency database releases and automatically updates its internal list.
    """
    variants = []

    for decl in (
        AccountType,
        AutomatedDecision,
        ContentType,
        DecisionAccount,
        DecisionGround,
        DecisionMonetary,
        DecisionProvision,
        DecisionVisibility,
        InformationSource,
    ):
        variants.extend(decl.variant_names())

    for names in (
        Keyword,
        PlatformNames,
        StatementCategory,
        YesNo,
    ):
        variants.extend(names)

    variants.extend(
        set(ContentLanguage.__members__.keys()).union(
            TerritorialScope.__members__.keys()
        )
    )

    return variants


VariantValueType = pl.Enum(_all_variants())


VariantTooValueType = pl.Enum(Keyword)


StatisticsSchema = pl.Schema({
    "start_date": pl.Date,
    "end_date": pl.Date,
    "tag": pl.String,
    "column": ColumnValueType,
    "entity": EntityValueType,
    "variant": VariantValueType,
    "variant_too": VariantTooValueType,
    "count": pl.Int64,
    "min": pl.Int64,
    "mean": pl.Int64,
    "max": pl.Int64,
})
"""
The schema for the summary statistics. Durations are encoded min/mean/max values
of the corresponding milliseconds.
"""


# ======================================================================================


def normalize_category(category: str) -> str:
    """Normalize the given category to a schema-approved one."""
    cat = category.upper()
    if cat.startswith("CATEGORY_"):
        cat = f"STATEMENT_{cat}"
    elif not cat.startswith("STATEMENT_CATEGORY_"):
        cat = f"STATEMENT_CATEGORY_{cat}"
    if cat not in StatementCategory:
        raise ValueError(f'"{category}" does not match any valid statement categories')
    return cat


KEYWORDS_V1 = frozenset([
    # --- Animal welfare
    "KEYWORD_ANIMAL_HARM",
    "KEYWORD_UNLAWFUL_SALE_ANIMALS",

    # --- Data protection and privacy violations
    "KEYWORD_BIOMETRIC_DATA_BREACH",
    "KEYWORD_MISSING_PROCESSING_GROUND",
    "KEYWORD_RIGHT_TO_BE_FORGOTTEN",
    "KEYWORD_DATA_FALSIFICATION",

    # --- Illegal or harmful speech
    "KEYWORD_DEFAMATION",
    "KEYWORD_DISCRIMINATION",
    "KEYWORD_HATE_SPEECH",

    # --- Intellectual property infringements
    "KEYWORD_COPYRIGHT_INFRINGEMENT",
    "KEYWORD_DESIGN_INFRINGEMENT",
    "KEYWORD_GEOGRAPHIC_INDICATIONS_INFRINGEMENT",
    "KEYWORD_PATENT_INFRINGEMENT",
    "KEYWORD_TRADE_SECRET_INFRINGEMENT",
    "KEYWORD_TRADEMARK_INFRINGEMENT",

    # --- Negative effects on civic discourse or elections
    "KEYWORD_DISINFORMATION", # v1 (replaced)
    "KEYWORD_MISINFORMATION", # v1 (replaced)
    "KEYWORD_FOREIGN_INFORMATION_MANIPULATION", # v1 (removed)

    # --- Non-consensual behavior
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING", # v1 (moved)
    "KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE", # v1 (moved, renamed)
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION", # v1 (moved, renamed)
    "KEYWORD_STALKING",

    # --- Pornography or sexualized content
    "KEYWORD_ADULT_SEXUAL_MATERIAL",
    "KEYWORD_IMAGE_BASED_SEXUAL_ABUSE", # v1 (removed)

    # --- Protection of minors
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL_DEEPFAKE",
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS",
    "KEYWORD_UNSAFE_CHALLENGES",

    # --- Risk for public security
    "KEYWORD_ILLEGAL_ORGANIZATIONS",
    "KEYWORD_RISK_ENVIRONMENTAL_DAMAGE",
    "KEYWORD_RISK_PUBLIC_HEALTH",
    "KEYWORD_TERRORIST_CONTENT",

    # --- Scams and/or fraud
    "KEYWORD_INAUTHENTIC_ACCOUNTS",
    "KEYWORD_INAUTHENTIC_LISTINGS",
    "KEYWORD_INAUTHENTIC_USER_REVIEWS",
    "KEYWORD_IMPERSONATION_ACCOUNT_HIJACKING",
    "KEYWORD_PHISHING",
    "KEYWORD_PYRAMID_SCHEMES",

    # --- Self-harm
    "KEYWORD_CONTENT_PROMOTING_EATING_DISORDERS",
    "KEYWORD_SELF_MUTILATION",
    "KEYWORD_SUICIDE",

    # --- Scope of platform service
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS",
    "KEYWORD_GEOGRAPHICAL_REQUIREMENTS",
    "KEYWORD_GOODS_SERVICES_NOT_PERMITTED",
    "KEYWORD_LANGUAGE_REQUIREMENTS",
    "KEYWORD_NUDITY",

    # --- Unsafe and/or illegal products
    "KEYWORD_INSUFFICIENT_INFORMATION_TRADERS", # v1 (moved, renamed)
    "KEYWORD_REGULATED_GOODS_SERVICES", # v1 (removed)
    "KEYWORD_DANGEROUS_TOYS", # v1 (removed)

    # --- Violence
    "KEYWORD_COORDINATED_HARM",
    "KEYWORD_GENDER_BASED_VIOLENCE", # v1 (removed)
    "KEYWORD_HUMAN_EXPLOITATION",
    "KEYWORD_HUMAN_TRAFFICKING",
    "KEYWORD_INCITEMENT_VIOLENCE_HATRED",

    # --- Other
    "KEYWORD_OTHER",
])


KEYWORDS_V2 = frozenset([
    # --- Animal welfare
    "KEYWORD_ANIMAL_HARM",
    "KEYWORD_UNLAWFUL_SALE_ANIMALS",

    # --- Consumer information (v2)
    "KEYWORD_HIDDEN_ADVERTISEMENT", # v2 (added)
    "KEYWORD_INSUFFICIENT_INFORMATION_ON_TRADERS", # v2 (moved, renamed)
    "KEYWORD_MISLEADING_INFO_CONSUMER_RIGHTS", # v2 (added)
    "KEYWORD_MISLEADING_INFO_GOODS_SERVICES", # v2 (added)
    "KEYWORD_NONCOMPLIANCE_PRICING", # v2 (added)

    # --- Cyber violence (v2)
    "KEYWORD_CYBER_BULLYING_INTIMIDATION", # v2 (added)
    "KEYWORD_CYBER_HARASSMENT", # v2 (added)
    "KEYWORD_CYBER_INCITEMENT", # v2 (added)
    "KEYWORD_CYBER_STALKING", # v2 (added)
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING", # v2 (moved)
    "KEYWORD_NON_CONSENSUAL_MATERIAL_DEEPFAKE", # v2 (moved, renamed)

    # --- Cyber violence against women (v2)
    "KEYWORD_BULLYING_AGAINST_GIRLS", # v2 (added)
    "KEYWORD_CYBER_HARASSMENT_AGAINST_WOMEN", # v2 (added)
    "KEYWORD_CYBER_STALKING_AGAINST_WOMEN", # v2 (added)
    "KEYWORD_FEMALE_GENDERED_DISINFORMATION", # v2 (added)
    "KEYWORD_INCITEMENT_AGAINST_WOMEN", # v2 (added)
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING_AGAINST_WOMEN", # v2 (added)
    "KEYWORD_NON_CONSENSUAL_MATERIAL_DEEPFAKE_AGAINST_WOMEN", # v2 (added)

    # --- Data protection and privacy violations
    "KEYWORD_BIOMETRIC_DATA_BREACH",
    "KEYWORD_MISSING_PROCESSING_GROUND",
    "KEYWORD_RIGHT_TO_BE_FORGOTTEN",
    "KEYWORD_DATA_FALSIFICATION",

    # --- Illegal or harmful speech
    "KEYWORD_DEFAMATION",
    "KEYWORD_DISCRIMINATION",
    "KEYWORD_HATE_SPEECH",

    # --- Intellectual property infringements
    "KEYWORD_COPYRIGHT_INFRINGEMENT",
    "KEYWORD_DESIGN_INFRINGEMENT",
    "KEYWORD_GEOGRAPHIC_INDICATIONS_INFRINGEMENT",
    "KEYWORD_PATENT_INFRINGEMENT",
    "KEYWORD_TRADE_SECRET_INFRINGEMENT",
    "KEYWORD_TRADEMARK_INFRINGEMENT",

    # --- Negative effects on civic discourse or elections
    "KEYWORD_MISINFORMATION_DISINFORMATION", # v2 (replacement)
    "KEYWORD_VIOLATION_EU_LAW", # v2 (added)
    "KEYWORD_VIOLATION_NATIONAL_LAW", # v2 (added)

    # --- Non-consensual behavior
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION",
    "KEYWORD_STALKING",

    # --- Pornography or sexualized content
    "KEYWORD_ADULT_SEXUAL_MATERIAL",

    # --- Protection of minors
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL_DEEPFAKE",
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS",
    "KEYWORD_UNSAFE_CHALLENGES",

    # --- Risk for public security
    "KEYWORD_ILLEGAL_ORGANIZATIONS",
    "KEYWORD_RISK_ENVIRONMENTAL_DAMAGE",
    "KEYWORD_RISK_PUBLIC_HEALTH",
    "KEYWORD_TERRORIST_CONTENT",

    # --- Scams and/or fraud
    "KEYWORD_INAUTHENTIC_ACCOUNTS",
    "KEYWORD_INAUTHENTIC_LISTINGS",
    "KEYWORD_INAUTHENTIC_USER_REVIEWS",
    "KEYWORD_IMPERSONATION_ACCOUNT_HIJACKING",
    "KEYWORD_PHISHING",
    "KEYWORD_PYRAMID_SCHEMES",

    # --- Self-harm
    "KEYWORD_CONTENT_PROMOTING_EATING_DISORDERS",
    "KEYWORD_SELF_MUTILATION",
    "KEYWORD_SUICIDE",

    # --- Scope of platform service
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS",
    "KEYWORD_GEOGRAPHICAL_REQUIREMENTS",
    "KEYWORD_GOODS_SERVICES_NOT_PERMITTED",
    "KEYWORD_LANGUAGE_REQUIREMENTS",
    "KEYWORD_NUDITY",

    # --- Unsafe and/or illegal products
    "KEYWORD_PROHIBITED_PRODUCTS", # v2 (added)
    "KEYWORD_UNSAFE_PRODUCTS", # v2 (added)

    # --- Violence
    "KEYWORD_COORDINATED_HARM",
    "KEYWORD_HUMAN_EXPLOITATION",
    "KEYWORD_HUMAN_TRAFFICKING",
    "KEYWORD_INCITEMENT_VIOLENCE_HATRED",
    "KEYWORD_TRAFFICKING_WOMEN_GIRLS", # v2 (added)

    # --- Other
    "KEYWORD_OTHER",
])
