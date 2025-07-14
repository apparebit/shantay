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
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
import datetime as dt
import enum
import itertools
from types import GenericAlias, MappingProxyType
from typing import Any, cast, get_args, get_origin, Literal, overload, Self

import polars as pl

from .color import (
    BLUE, CYAN, GRAY, GREEN, LIGHT_BLUE, MAGENTA, OLIVE, ORANGE, PALETTE, PINK, PURPLE,
    RED
)


# ======================================================================================
# Humanized statement categories and keywords


_HUMANIZED_FRAGMENTS = {
    " And ": " and ",
    " Based ": "-Based ",
    " Eu ": " EU ",
    " For ": " for ",
    " Non ": " Non-",
    " Of ": " of ",
    " On ": " on ",
    " Or ": " or ",
    " Specific ": "-Specific ",
    " To ": " to ",
}


_HUMANIZED_TAGS = {
    "Eea": "EU+IS+LI+NO",
    "Eea No Is": "EU+LI+NO",
}


def humanize(tag: None | str) -> str:
    """Generate a humane presentation for the given tag."""
    if tag is None:
        return "—none—"
    if tag.startswith("STATEMENT_CATEGORY_"):
        tag = tag[len("STATEMENT_CATEGORY_"):]
    elif tag.startswith("KEYWORD_"):
        tag = tag[len("KEYWORD_"):]
    tag = tag.replace("_", " ")
    if len(tag) != 2 or tag != tag.upper():
        tag = tag.title()
        for source, target in _HUMANIZED_FRAGMENTS.items():
            tag = tag.replace(source, target)
    tag = _HUMANIZED_TAGS.get(tag, tag)
    return tag


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


type VariantNamesAndColors = dict[None | str, tuple[str, str]]


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

    def has_many_variants(self) -> bool:
        return 10 <= len(self.variants)

    def has_null_variant(self) -> bool:
        return None in self.variants

    def is_duration(self) -> bool:
        return self.quant_label == "Days"

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

    def without_null(self) -> Self:
        """Recreate the metric without a null variant."""
        return type(self)(
            self.field,
            self.label,
            {k: v for k, v in self.variants.items() if k is not None},
            selector=self.selector,
            quantity=self.quantity,
            quant_label=self.quant_label,
        )

    def with_variants(
        self,
        names: Iterable[None | str],
        use_palette: bool = False,
    ) -> Self:
        """
        Create a new metric declaration that has the same fields as this one,
        except that the variants only include the names, in that order. The
        colors can be the original ones or be drawn from the standard palette.
        """
        if use_palette:
            color_count = len(PALETTE) - 1
            variants = {
                key: (
                    self.variants[key][0],
                    GRAY if key is None else PALETTE[index % color_count]
                )
                for index, key in enumerate(names)
            }
        else:
            variants = {key: self.variants[key] for key in names}

        return type(self)(
            self.field,
            self.label,
            variants,
            selector=self.selector,
            quantity=self.quantity,
            quant_label=self.quant_label,
        )


def make_metric(
    field: str,
    label: str,
    variants: Iterable[None | str],
    quant_label: str = "Statements of Reasons",
) -> MetricDeclaration:
    color_count = len(PALETTE) - 1
    variant_decl = cast(VariantNamesAndColors, {
        v: (humanize(v), GRAY if v is None else PALETTE[i % color_count])
        for i, v in enumerate(variants)
    })
    return MetricDeclaration(field, label, variant_decl, quant_label=quant_label)


# --------------------------------------------------------------------------------------


AccountTypeMetric = MetricDeclaration("account_type", "Account Types", {
    "ACCOUNT_TYPE_BUSINESS": ("Business", ORANGE),
    "ACCOUNT_TYPE_PRIVATE": ("Individual", BLUE),
    None: ("—none—", GRAY),
})


AutomatedDecisionMetric = MetricDeclaration("automated_decision", "Automated Decisions", {
    "AUTOMATED_DECISION_FULLY": ("Fully Automated", CYAN),
    "AUTOMATED_DECISION_PARTIALLY": ("Partially Automated", BLUE),
    "AUTOMATED_DECISION_NOT_AUTOMATED": ("Not Automated", GREEN),
    None: ("—none—", GRAY),
})


AutomatedDetectionMetric = MetricDeclaration("automated_detection", "Automated Detection", {
    "Yes": ("Automated", LIGHT_BLUE),
    "No": ("Not Automated", PURPLE),
    None: ("—none—", GRAY),
})


ContentLanguageMetric = make_metric("content_language", "Content Language",
    itertools.chain(
        (item.name for item in ContentLanguage),
        [None],
    ),
)


ContentTypeMetric = MetricDeclaration("content_type", "Content Types", {
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


DecisionAccountMetric = MetricDeclaration("decision_account", "Account Decisions", {
    "DECISION_ACCOUNT_SUSPENDED": ("Suspended", ORANGE),
    "DECISION_ACCOUNT_TERMINATED": ("Terminated", RED),
    None: ("—none—", GRAY),
})


DecisionGroundMetric = MetricDeclaration("decision_ground", "Decision Grounds", {
    "DECISION_GROUND_ILLEGAL_CONTENT": ("Illegal", RED),
    "DECISION_GROUND_INCOMPATIBLE_CONTENT": ("Incompatible", ORANGE),
})


DecisionMonetaryMetric = MetricDeclaration("decision_monetary", "Monetary Decisions", {
   "DECISION_MONETARY_SUSPENSION": ("Suspended", ORANGE),
   "DECISION_MONETARY_TERMINATION": ("Terminated", RED),
   "DECISION_MONETARY_OTHER": ("Other", PINK),
   None: ("—none—", GRAY),
})


DecisionProvisionMetric = MetricDeclaration("decision_provision", "Service Provision Decisions", {
    "DECISION_PROVISION_PARTIAL_SUSPENSION": ("Partially Suspended", LIGHT_BLUE),
    "DECISION_PROVISION_TOTAL_SUSPENSION": ("Suspended", BLUE),
    "DECISION_PROVISION_PARTIAL_TERMINATION": ("Partially Terminated", ORANGE),
    "DECISION_PROVISION_TOTAL_TERMINATION": ("Terminated", RED),
    None: ("—none—", GRAY),
})


DecisionTypeMetric = MetricDeclaration("decision_type", "Decision Types", {
    "vis": ("Visibility", BLUE),
    "mon": ("Monetary", OLIVE),
    "vis_mon": ("Visibility & Monetary", MAGENTA),
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
    "is_null": ("—none—", GRAY),
}, selector="entity")


DecisionVisibilityMetric = MetricDeclaration("decision_visibility", "Visibility Decisions", {
    "DECISION_VISIBILITY_CONTENT_REMOVED": ("Removed", LIGHT_BLUE),
    "DECISION_VISIBILITY_CONTENT_DISABLED": ("Disabled", RED),
    "DECISION_VISIBILITY_CONTENT_DEMOTED": ("Demoted", ORANGE),
    "DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED": ("Age-Restricted", GREEN),
    "DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED": ("Interaction Restricted", PURPLE),
    "DECISION_VISIBILITY_CONTENT_LABELLED": ("Labelled", PINK),
    "DECISION_VISIBILITY_OTHER": ("Other", BLUE),
    None: ("—none—", GRAY),
})


IncompatibleContentIllegalMetric = MetricDeclaration(
    "incompatible_content_illegal",
    "Incompatible Is Illegal", {
        "Yes": ("Yes", RED),
        "No": ("No", GREEN),
        None: ("—none—", GRAY),
    }
)


InformationSourceMetric = MetricDeclaration("source_type", "Information Sources", {
    "SOURCE_ARTICLE_16": ("Article 16", LIGHT_BLUE),
    "SOURCE_TRUSTED_FLAGGER": ("Trusted Flagger", BLUE),
    "SOURCE_TYPE_OTHER_NOTIFICATION": ("Other Notification", ORANGE),
    "SOURCE_VOLUNTARY": ("Voluntary", GREEN),
    None: ("—none—", GRAY),
})


from ._keyword import (
    KeywordChildSexualAbuseMaterial as KeywordChildSexualAbuseMaterial,
    Keyword as Keyword
)


from ._platform import (
    CanonicalPlatformNames as CanonicalPlatformNames,
    check_db_platforms as check_db_platforms,
    check_stats_platforms as check_stats_platforms,
    MetaPlatforms as MetaPlatforms,
    MissingPlatformError as MissingPlatformError,
    PlatformNames as PlatformNames,
    sync_web_platforms as sync_web_platforms,
    update_platforms as update_platforms,
)


ModerationDelayMetric = MetricDeclaration(
    "moderation_delay",
    "Moderation Delays",
    {},
    selector="column",
    quantity="mean",
    quant_label="Days",
)


ProcessingDelayMetric = MetricDeclaration(
    ["moderation_delay", "disclosure_delay"],
    "Delays",
    {
        "moderation_delay": ("Moderation", LIGHT_BLUE),
        "disclosure_delay": ("Disclosure", ORANGE),
        #"release_delay": ("Release", RED),
        None: ("—none—", GRAY),
    },
    selector="column",
    quantity="mean",
    quant_label="Days",
)


from ._category import (
    StatementCategoryProtectionOfMinors as StatementCategoryProtectionOfMinors,
    StatementCategory as StatementCategory,
)


CategoryMetric = make_metric(
    "category", "Category", StatementCategory, quant_label="SoRs with Category"
)


StatementCountMetric = MetricDeclaration(
    "rows",
    "Statement Counts",
    {},
    selector="column",
)


TerritorialScopeMetric = make_metric("territorial_scope", "Territorial Scope",
    itertools.chain(
        (item.name for item in TerritorialScope),
        [None],
    ),
)


YesNo = (
    "Yes",
    "No",
)


# ======================================================================================
# Schemata


FIELDS = MappingProxyType({
    "uuid": str,

    "decision_visibility": list[DecisionVisibilityMetric],
    "decision_visibility_other": str,
    "end_date_visibility_restriction": dt.datetime,

    "decision_monetary": DecisionMonetaryMetric,
    "decision_monetary_other": str,
    "end_date_monetary_restriction": dt.datetime,

    "decision_provision": DecisionProvisionMetric,
    "end_date_service_restriction": dt.datetime,

    "decision_account": DecisionAccountMetric,
    "end_date_account_restriction": dt.datetime,

    "account_type": AccountTypeMetric,

    "decision_ground": DecisionGroundMetric,
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

    "content_type": list[ContentTypeMetric],
    "content_type_other": str,
    "content_language": tuple(v.name for v in ContentLanguage),
    "content_date": dt.datetime,

    "territorial_scope": list[tuple(v.name for v in TerritorialScope)],
    "application_date": dt.datetime,
    "decision_facts": str,

    "source_type": InformationSourceMetric,
    "source_identity": str,
    "automated_detection": YesNo,
    "automated_decision": AutomatedDecisionMetric,

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


STRATIFY_BY_CATEGORY = False


class TransformType(enum.Enum):
    """Non-parametric transform types."""
    PLATFORM_NAME = enum.auto()
    CATEGORY_NAME = enum.auto()
    SKIPPED_DATE = enum.auto()
    ROWS = enum.auto()
    VALUE_COUNTS = enum.auto()
    TEXT_VALUE_COUNTS = enum.auto()
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


# The transforms cover all DSA transparency database entries without unconstrained text.
TRANSFORMS = {
    "rows": TransformType.ROWS,
    "decision_type": TransformType.DECISION_TYPE,
    "decision_visibility": ValueCountsPlusTransform(
        self_is_list=True, other_field="end_date_visibility_restriction"
    ),
    "decision_visibility_other": TransformType.TEXT_VALUE_COUNTS,
    "end_date_visibility_restriction": TransformType.SKIPPED_DATE,
    "visibility_restriction_duration": DurationTransform(
        "application_date", "end_date_visibility_restriction"
    ),
    "decision_monetary": ValueCountsPlusTransform(
        self_is_list=False, other_field="end_date_monetary_restriction"),
    "decision_monetary_other": TransformType.TEXT_VALUE_COUNTS,
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
    "category": (
        TransformType.CATEGORY_NAME if STRATIFY_BY_CATEGORY
        else TransformType.VALUE_COUNTS
    ),
    "category_addition": TransformType.LIST_VALUE_COUNTS,
    "category_specification": TransformType.LIST_VALUE_COUNTS,
    "category_specification_other": TransformType.TEXT_VALUE_COUNTS,
    "content_type": TransformType.LIST_VALUE_COUNTS,
    "content_type_other": TransformType.TEXT_VALUE_COUNTS,
    "content_language": TransformType.VALUE_COUNTS,
    "moderation_delay": DurationTransform("content_date", "application_date"),
    "territorial_scope": TransformType.LIST_VALUE_COUNTS,
    "disclosure_delay": DurationTransform("application_date", "created_at"),
    #"release_delay": DurationTransform("created_at", "released_on"),
    "source_type": TransformType.VALUE_COUNTS,
    "automated_detection": TransformType.VALUE_COUNTS,
    "automated_decision": TransformType.VALUE_COUNTS,
    "platform_name": TransformType.PLATFORM_NAME,
}

TRANSFORM_COUNT = sum(
    (0 if v in (TransformType.SKIPPED_DATE, TransformType.PLATFORM_NAME) else 1)
    for v in TRANSFORMS.values()
)


# ======================================================================================
# Statistics Schema


# For now, the universe of tags are all statement categories and keywords, with
# each keyword implying the larger category as well.
TagValueType = pl.Enum([*StatementCategory, *Keyword])


PlatformValueType = pl.Enum(PlatformNames)


CategoryValueType = pl.Enum(StatementCategory)


ColumnValueType = pl.Enum((
    "start_date",
    "end_date",
    "batch_count",
    "batch_rows",
    "batch_rows_with_keywords",
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
    #"release_delay",
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
    "null_bc_negative",
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
    variants = ["—none—"]

    for decl in (
        AccountTypeMetric,
        AutomatedDecisionMetric,
        ContentTypeMetric,
        DecisionAccountMetric,
        DecisionGroundMetric,
        DecisionMonetaryMetric,
        DecisionProvisionMetric,
        DecisionVisibilityMetric,
        InformationSourceMetric,
    ):
        variants.extend(decl.variant_names())

    for names in (
        Keyword,
        StatementCategory,
        YesNo,
    ):
        variants.extend(names)

    variants.extend(
        set(ContentLanguage.__members__.keys()).union(
            TerritorialScope.__members__.keys()
        )
    )

    # Impose well-defined order, since Pola.rs considers order for equality testing
    return sorted(variants)


VariantValueType = pl.Enum(_all_variants())


StatisticsSchema = pl.Schema({
    "start_date": pl.Date,
    "end_date": pl.Date,
    "tag": TagValueType,
    "platform": PlatformValueType,
} | (
    {"category": CategoryValueType} if STRATIFY_BY_CATEGORY else {}
) | {
    "column": ColumnValueType,
    "entity": EntityValueType,
    "variant": VariantValueType,
    "text": pl.String,
    "count": pl.Int64,
    "min": pl.Int64,
    "mean": pl.Int64,
    "max": pl.Int64,
})
"""
The schema for the summary statistics. Durations are encoded min, mean, and max
values of the corresponding integral milliseconds as well as the count of
durations contributing to the three statistics. The latter enables the
aggregation of means. Since durations are computed from the difference of two
date/times, shantay may have to correct for negative durations. It tracks the
number of these corrections as well.
"""

# Columns with arbitrary text included in the statistics
TextColumns = (
    "decision_visibility_other",
    "decision_monetary_other",
    "category_specification_other",
    "content_type_other",
)


# ======================================================================================


_CATEGORY_FILE_NAMES = frozenset(
    c[len("STATEMENT_CATEGORY_"):].lower().replace("_", "-") for c in StatementCategory
)

def is_category_file(name: str) -> bool:
    return name in _CATEGORY_FILE_NAMES


@overload
def normalize_category(category: None) -> None: ...

@overload
def normalize_category(category: str) -> str: ...

def normalize_category(category: None | str) -> None | str:
    """Normalize the given category to a schema-approved one."""
    if category is None:
        return None
    cat = category.upper().replace("-", "_").replace(" ", "_")
    if cat.startswith("CATEGORY_"):
        cat = f"STATEMENT_{cat}"
    elif not cat.startswith("STATEMENT_CATEGORY_"):
        cat = f"STATEMENT_CATEGORY_{cat}"
    if cat not in StatementCategory:
        raise ValueError(f'"{category}" does not match any valid statement categories')
    return cat


@overload
def normalize_keyword(keyword: None) -> None: ...

@overload
def normalize_keyword(keyword: str) -> str: ...

def normalize_keyword(keyword: None | str) -> None | str:
    if keyword is None:
        return None
    key = keyword.upper().replace("-", "_").replace(" ", "_")
    if not key.startswith("KEYWORD_"):
        key = f"KEYWORD_{key}"
    if key not in Keyword:
        raise ValueError(f'"{keyword}" does not match any valid keyword')
    return key
