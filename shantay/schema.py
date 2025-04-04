"""
Schemata

Shantay uses `SCHEMA` as its schema for transparency database records. That
schema is as tight as possible, using specific enumerations where they are
documented. But to successfully ingest database records, the tool also uses the
weaker schemas `PARTIAL_SCHEMA` and `BASE_SCHEMA`. Data frames read in with
either schema are incrementally transformed to the tighter main schema.
Meanwhile, statistics data needs to combine many different enumerations in the
same column and hence uses more relaxed type constraints.
"""
import datetime as dt
import enum
from types import GenericAlias, MappingProxyType
from typing import Any, get_args, get_origin

import polars as pl


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


AccountType = (
    "ACCOUNT_TYPE_BUSINESS",
    "ACCOUNT_TYPE_PRIVATE",
)


AutomatedDecision = (
    "AUTOMATED_DECISION_FULLY",
    "AUTOMATED_DECISION_PARTIALLY",
    "AUTOMATED_DECISION_NOT_AUTOMATED",
)


ContentType = (
    "CONTENT_TYPE_APP",
    "CONTENT_TYPE_AUDIO",
    "CONTENT_TYPE_IMAGE",
    "CONTENT_TYPE_PRODUCT",
    "CONTENT_TYPE_SYNTHETIC_MEDIA",
    "CONTENT_TYPE_TEXT",
    "CONTENT_TYPE_VIDEO",
    "CONTENT_TYPE_OTHER",
)


DecisionAccount = (
    "DECISION_ACCOUNT_SUSPENDED",
    "DECISION_ACCOUNT_TERMINATED",
)


DecisionGround = (
    "DECISION_GROUND_ILLEGAL_CONTENT",
    "DECISION_GROUND_INCOMPATIBLE_CONTENT",
)


DecisionMonetary = (
   "DECISION_MONETARY_SUSPENSION",
   "DECISION_MONETARY_TERMINATION",
   "DECISION_MONETARY_OTHER",
)


DecisionProvision = (
    "DECISION_PROVISION_PARTIAL_SUSPENSION",
    "DECISION_PROVISION_TOTAL_SUSPENSION",
    "DECISION_PROVISION_PARTIAL_TERMINATION",
    "DECISION_PROVISION_TOTAL_TERMINATION",
)


DecisionVisibility = (
    "DECISION_VISIBILITY_CONTENT_REMOVED",
    "DECISION_VISIBILITY_CONTENT_DISABLED",
    "DECISION_VISIBILITY_CONTENT_DEMOTED",
    "DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED",
    "DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED",
    "DECISION_VISIBILITY_CONTENT_LABELLED",
    "DECISION_VISIBILITY_OTHER",
)


InformationSource = (
    "SOURCE_ARTICLE_16",
    "SOURCE_TRUSTED_FLAGGER",
    "SOURCE_TYPE_OTHER_NOTIFICATION",
    "SOURCE_VOLUNTARY",
)


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


PlatformName = (
    "AliExpress",
    "Badoo",
    "Booking.com",
    "Bumble",
    "Campfire",
    "Canva",
    "Chrome Web Store",
    "Dailymotion",
    "Discord Netherlands B.V.",
    "Facebook",
    "Google Maps",
    "Google Play",
    "Google Shopping",
    "Habbo",
    "Hinge",
    "Hotel Hideaway",
    "Idealo",
    "Instagram",
    "Kleinanzeigen",
    "Meetic SAS",
    "OTTO Market",
    "Pinterest",
    "Pornhub",
    "Quora Ireland Limited",
    "Rajče",
    "Reddit",
    "Roblox",
    "Snapchat",
    "Stripchat",
    "Temu",
    "Threads",
    "TikTok",
    "Tinder",
    "VSCO",
    "Vinted UAB",
    "Wallapop",
    "WhatsApp Channels",
    "X",
    "YouTube",
    "Zalando",
    "bolha.com",
    "leboncoin",
    "willhaben internet service GmbH & Co KG",
    "www.gutefrage.net",
)


CANONICAL_PLATFORM_NAMES = MappingProxyType({
    "Discord Netherlands B.V.": "Discord",
    "OTTO Market": "OTTO",
    "Quora Ireland Limited": "Quora",
    "WhatsApp Channels": "WhatsApp",
    "willhaben internet service GmbH & Co KG": "willhaben",
    "www.gutefrage.net": "gutefrage.net"
})


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
    #"release_on": dt.date,
})


def polarize(ptype: GenericAlias | tuple[str, ...] | type) -> Any:
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

    origin = get_origin(ptype)
    args = get_args(ptype)

    if origin is list:
        if len(args) == 1 and not isinstance(args[0], str):
            return pl.List(polarize(args[0]))

        return pl.List(polarize(args))

    raise ValueError(f'cannot convert "{ptype}"')


def _generate_schemata() -> tuple[pl.Schema, pl.Schema, pl.Schema]:
    partial = {}
    base = {}
    full = {}

    for name, ptype in FIELDS.items():
        dtype = polarize(ptype)
        is_enum = isinstance(dtype, pl.Enum)

        if is_enum and name != "content_language":
            partial[name] = dtype

        if is_enum:
            base[name] = dtype
        else:
            base[name] = pl.String

        full[name] = dtype

    return pl.Schema(partial), pl.Schema(base), pl.Schema(full)

PARTIAL_SCHEMA, BASE_SCHEMA, SCHEMA = _generate_schemata()
del _generate_schemata


ColumnValueType = pl.Enum((
    "start_date",
    "end_date",
    "batch_count",
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


STATISTICS_SCHEMA = pl.Schema({
    "start_date": pl.Date,
    "end_date": pl.Date,
    "tag": pl.Categorical(),
    "column": ColumnValueType,
    "entity": pl.Categorical(),
    "variant": pl.Categorical(),
    "count": pl.UInt64,
    "min": pl.UInt64,
    "mean": pl.UInt64,
    "max": pl.UInt64,
})


# ======================================================================================


KEYWORDS_MINOR_PROTECTION = MappingProxyType({
    "NO_KEYWORD": "None",
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS": "Age-Restricted",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL": "CSAM",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL_DEEPFAKE": "Deepfake",
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS": "Grooming",
    "KEYWORD_UNSAFE_CHALLENGES": "Unsafe Challenges",
    "KEYWORD_OTHER": "Other",
})


# Plus keywords found to overlap in practice
KEYWORDS_MINOR_PROTECTION_PLUS = MappingProxyType(KEYWORDS_MINOR_PROTECTION | {
    "KEYWORD_ADULT_SEXUAL_MATERIAL": "Adult Material",
    "KEYWORD_HATE_SPEECH": "Hate Speech",
    "KEYWORD_HUMAN_TRAFFICKING": "Trafficking",
    "KEYWORD_NUDITY": "Nudity",
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION": "Bullying",
    "KEYWORD_REGULATED_GOODS_SERVICES": "Regulated",
})


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
