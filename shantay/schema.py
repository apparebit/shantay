import enum
import polars as pl

DecisionVisibility = pl.Enum([
    "DECISION_VISIBILITY_CONTENT_REMOVED",
    "DECISION_VISIBILITY_CONTENT_DISABLED",
    "DECISION_VISIBILITY_CONTENT_DEMOTED",
    "DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED",
    "DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED",
    "DECISION_VISIBILITY_CONTENT_LABELLED",
    "DECISION_VISIBILITY_OTHER",
])

DecisionMonetary = pl.Enum([
   "DECISION_MONETARY_SUSPENSION",
   "DECISION_MONETARY_TERMINATION",
   "DECISION_MONETARY_OTHER",
])

DecisionProvision = pl.Enum([
    "DECISION_PROVISION_PARTIAL_SUSPENSION",
    "DECISION_PROVISION_TOTAL_SUSPENSION",
    "DECISION_PROVISION_PARTIAL_TERMINATION",
    "DECISION_PROVISION_TOTAL_TERMINATION",
])

DecisionAccount = pl.Enum([
    "DECISION_ACCOUNT_SUSPENDED",
    "DECISION_ACCOUNT_TERMINATED",
])

AccountType = pl.Enum([
    "ACCOUNT_TYPE_BUSINESS",
    "ACCOUNT_TYPE_PRIVATE",
])

DecisionGround = pl.Enum([
    "DECISION_GROUND_ILLEGAL_CONTENT",
    "DECISION_GROUND_INCOMPATIBLE_CONTENT",
])

ContentType = pl.Enum([
    "CONTENT_TYPE_APP",
    "CONTENT_TYPE_AUDIO",
    "CONTENT_TYPE_IMAGE",
    "CONTENT_TYPE_PRODUCT",
    "CONTENT_TYPE_SYNTHETIC_MEDIA",
    "CONTENT_TYPE_TEXT",
    "CONTENT_TYPE_VIDEO",
    "CONTENT_TYPE_OTHER",
])

# See
# https://transparency.dsa.ec.europa.eu/page/additional-explanation-for-statement-attributes
# for two-level classification for types of violative activity.


StatementCategory = pl.Enum([
    "STATEMENT_CATEGORY_ANIMAL_WELFARE",
    "STATEMENT_CATEGORY_CONSUMER_INFORMATION", # v2 (added)
    "STATEMENT_CATEGORY_CYBER_VIOLENCE", # v2 (added)
    "STATEMENT_CATEGORY_CYBER_VIOLENCE_AGAINST_WOMEN", # v2 (added)
    "STATEMENT_CATEGORY_DATA_PROTECTION_AND_PRIVACY_VIOLATIONS",
    "STATEMENT_CATEGORY_ILLEGAL_OR_HARMFUL_SPEECH",
    "STATEMENT_CATEGORY_INTELLECTUAL_PROPERTY_INFRINGEMENTS",
    "STATEMENT_CATEGORY_NEGATIVE_EFFECTS_ON_CIVIC_DISCOURSE_OR_ELECTIONS",
    "STATEMENT_CATEGORY_NON_CONSENSUAL_BEHAVIOUR", # v1 (deleted)
    "STATEMENT_CATEGORY_NOT_SPECIFIED_NOTICE", # v2 (added)
    "STATEMENT_CATEGORY_PORNOGRAPHY_OR_SEXUALIZED_CONTENT", # v1 (deleted)
    "STATEMENT_CATEGORY_PROTECTION_OF_MINORS",
    "STATEMENT_CATEGORY_RISK_FOR_PUBLIC_SECURITY",
    "STATEMENT_CATEGORY_SCAMS_AND_FRAUD",
    "STATEMENT_CATEGORY_SELF_HARM",
    # v2: STATEMENT_CATEGORY_OTHER_VIOLATION_TC
    "STATEMENT_CATEGORY_SCOPE_OF_PLATFORM_SERVICE", # v1 (renamed)
    # v1: STATEMENT_CATEGORY_SCOPE_OF_PLATFORM_SERVICE
    "STATEMENT_CATEGORY_OTHER_VIOLATION_TC", # v2 (renamed)
    # v2: STATEMENT_CATEGORY_UNSAFE_AND_PROHIBITED_PRODUCTS
    "STATEMENT_CATEGORY_UNSAFE_AND_ILLEGAL_PRODUCTS", # v1 (renamed)
    # v1: STATEMENT_CATEGORY_UNSAFE_AND_ILLEGAL_PRODUCTS
    "STATEMENT_CATEGORY_UNSAFE_AND_PROHIBITED_PRODUCTS", # v2 (renamed)
    "STATEMENT_CATEGORY_VIOLENCE",
])


def normalize_category(category: str) -> str:
    """Normalize the given category to a schema-approved one."""
    cat = category.upper()
    if cat.startswith("CATEGORY_"):
        cat = f"STATEMENT_{cat}"
    elif not cat.startswith("STATEMENT_CATEGORY_"):
        cat = f"STATEMENT_CATEGORY_{cat}"
    if cat not in StatementCategory.categories:
        raise ValueError(f'"{category}" does not match any valid statement categories')
    return cat


Keyword = pl.Enum([
    # --- Animal welfare
    "KEYWORD_ANIMAL_HARM",
    "KEYWORD_UNLAWFUL_SALE_ANIMALS",

    # --- Consumer information (v2)
    "KEYWORD_HIDDEN_ADVERTISEMENT", # v2 (added)
    # v1: KEYWORD_INSUFFICIENT_INFORMATION_TRADERS, Unsafe and/or illegal products
    "KEYWORD_INSUFFICIENT_INFORMATION_ON_TRADERS", # v2 (moved, renamed)
    "KEYWORD_MISLEADING_INFO_CONSUMER_RIGHTS", # v2 (added)
    "KEYWORD_MISLEADING_INFO_GOODS_SERVICES", # v2 (added)
    "KEYWORD_NONCOMPLIANCE_PRICING", # v2 (added)

    # --- Cyber violence (v2)
    "KEYWORD_CYBER_BULLYING_INTIMIDATION", # v2 (added)
    "KEYWORD_CYBER_HARASSMENT", # v2 (added)
    "KEYWORD_CYBER_INCITEMENT", # v2 (added)
    "KEYWORD_CYBER_STALKING", # v2 (added)
    # v1: -same-, Non-consensual behavior
    "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING", # v2 (moved)
    # v1: KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE, Non-consensual behavior
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
    "KEYWORD_DISINFORMATION", # v1 (replaced)
    "KEYWORD_MISINFORMATION", # v1 (replaced)
    "KEYWORD_MISINFORMATION_DISINFORMATION", # v2 (replacement)
    "KEYWORD_VIOLATION_EU_LAW", # v2 (added)
    "KEYWORD_VIOLATION_NATIONAL_LAW", # v2 (added)
    "KEYWORD_FOREIGN_INFORMATION_MANIPULATION", # v1 (removed)

    # --- Non-consensual behavior
    # v2: -same-, Cyber violence
    # "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING",
    # v2: KEYWORD_NON_CONSENSUAL_MATERIAL_DEEPFAKE, Cyber violence
    "KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE", # v1 (moved, renamed)
    # v2: KEYWORD_CYBER_BULLYING_INTIMIDATION, Cyber violence
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
    # v2: KEYWORD_INSUFFICIENT_INFORMATION_ON_TRADERS, Consumer information
    "KEYWORD_INSUFFICIENT_INFORMATION_TRADERS",
    "KEYWORD_PROHIBITED_PRODUCTS", # v2 (added)
    "KEYWORD_UNSAFE_PRODUCTS", # v2 (added)
    "KEYWORD_REGULATED_GOODS_SERVICES", # v1 (removed)
    "KEYWORD_DANGEROUS_TOYS", # v1 (removed)

    # --- Violence
    "KEYWORD_COORDINATED_HARM",
    "KEYWORD_GENDER_BASED_VIOLENCE", # v1 (removed)
    "KEYWORD_HUMAN_EXPLOITATION",
    "KEYWORD_HUMAN_TRAFFICKING",
    "KEYWORD_INCITEMENT_VIOLENCE_HATRED",
    "KEYWORD_TRAFFICKING_WOMEN_GIRLS", # v2 (added)

    # --- Other
    "KEYWORD_OTHER",
])


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


KEYWORDS_MINOR_PROTECTION = tuple([
    "NO_KEYWORD",
    "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL",
    "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL_DEEPFAKE",
    "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS",
    "KEYWORD_UNSAFE_CHALLENGES",
    "KEYWORD_OTHER",
])

# Keywords found to overlap in practice
EXTRA_KEYWORDS_MINOR_PROTECTION = tuple([
    "KEYWORD_ADULT_SEXUAL_MATERIAL",
    "KEYWORD_HATE_SPEECH",
    "KEYWORD_HUMAN_TRAFFICKING",
    "KEYWORD_NUDITY",
    "KEYWORD_ONLINE_BULLYING_INTIMIDATION",
    "KEYWORD_REGULATED_GOODS_SERVICES",
])

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


TerritorialScopeType = pl.Enum([c.name for c in TerritorialScope])


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


ContentLanguageType = pl.Enum([c.name for c in ContentLanguage])


InformationSource = pl.Enum([
    "SOURCE_ARTICLE_16",
    "SOURCE_TRUSTED_FLAGGER",
    "SOURCE_TYPE_OTHER_NOTIFICATION",
    "SOURCE_VOLUNTARY",
])

AutomatedDecision = pl.Enum([
    "AUTOMATED_DECISION_FULLY",
    "AUTOMATED_DECISION_PARTIALLY",
    "AUTOMATED_DECISION_NOT_AUTOMATED",
])

YesNo = pl.Enum([
    "Yes",
    "No",
])


COLUMNS = tuple([
    "uuid",
    "decision_visibility",
    "decision_visibility_other",
    "end_date_visibility_restriction",
    "decision_monetary",
    "decision_monetary_other",
    "end_date_monetary_restriction",
    "decision_provision",
    "end_date_service_restriction",
    "decision_account",
    "end_date_account_restriction",
    "account_type",
    "decision_ground",
    "decision_ground_reference_url",
    "illegal_content_legal_ground",
    "illegal_content_explanation",
    "incompatible_content_ground",
    "incompatible_content_explanation",
    "incompatible_content_illegal",
    "category",
    "category_addition",
    "category_specification",
    "category_specification_other",
    "content_type",
    "content_type_other",
    "content_language",
    "content_date",
    "territorial_scope",
    "application_date",
    "decision_facts",
    "source_type",
    "source_identity",
    "automated_detection",
    "automated_decision",
    "platform_name",
    "platform_uid",
    "created_at",
])

SCHEMA_OVERRIDES = {
    "decision_monetary": DecisionMonetary,
    "decision_provision": DecisionProvision,
    "decision_account": DecisionAccount,
    "account_type": AccountType,
    "decision_ground": DecisionGround,
    "incompatible_content_illegal": YesNo,
    "category": StatementCategory,
    #"content_language": ContentLanguageType,
    "source_type": InformationSource,
    "automated_detection": YesNo,
    "automated_decision": AutomatedDecision,
}


def base_schema() -> pl.Schema:
    schema = {}
    for column in COLUMNS:
        override = SCHEMA_OVERRIDES.get(column)
        schema[column] = override if override else pl.String
    return pl.Schema(schema)

BASE_SCHEMA = base_schema()


SCHEMA = pl.Schema({
    "uuid": pl.String,

    "decision_visibility": pl.List(DecisionVisibility),
    "decision_visibility_other": pl.String,
    "end_date_visibility_restriction": pl.Datetime(time_unit="ms"),

    "decision_monetary": DecisionMonetary,
    "decision_monetary_other": pl.String,
    "end_date_monetary_restriction": pl.Datetime(time_unit="ms"),

    "decision_provision": DecisionProvision,
    "end_date_service_restriction": pl.Datetime(time_unit="ms"),

    "decision_account": DecisionAccount,
    "end_date_account_restriction": pl.Datetime(time_unit="ms"),

    "account_type": AccountType,

    "decision_ground": DecisionGround,
    "decision_ground_reference_url": pl.String,

    "illegal_content_legal_ground": pl.String,
    "illegal_content_explanation": pl.String,

    "incompatible_content_ground": pl.String,
    "incompatible_content_explanation": pl.String,
    "incompatible_content_illegal": YesNo,

    "category": StatementCategory,
    "category_addition": pl.List(StatementCategory),
    "category_specification": pl.List(Keyword),
    "category_specification_other": pl.String,

    "content_type": pl.List(ContentType),
    "content_type_other": pl.String,
    "content_language": ContentLanguageType,
    "content_date": pl.Datetime(time_unit="ms"),

    "territorial_scope": pl.List(TerritorialScopeType),
    "application_date": pl.Datetime(time_unit="ms"),
    "decision_facts": pl.String,

    "source_type": InformationSource,
    "source_identity": pl.String,
    "automated_detection": YesNo,
    "automated_decision": AutomatedDecision,

    "platform_name": pl.String,
    "platform_uid": pl.String,

    "created_at": pl.Datetime(time_unit="ms"),
})

# --------------------------------------------------------------------------------------

_EU = tuple([
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
])

_EEA = tuple([
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IS",
    "IT",
    "LI",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
])

_EEA_no_IS = tuple([
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IT",
    "LI",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
])

class CountryGroups:
    EU="[" + ",".join(f'"{c}"' for c in _EU) + "]"
    EEA="[" + ",".join(f'"{c}"' for c in _EEA) + "]"
    EEA_no_IS="[" + ",".join(f'"{c}"' for c in _EEA_no_IS) + "]"
