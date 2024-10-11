using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Shared.Geo.SummitsCalculator;
using Shared.Models;

namespace Shared.Benchmarks;

public class SummitsBenchmarks
{

    // The 66 peaks that SummitsWorker will fetch for this activity
    readonly static List<(string, Coordinate)> pointsA =
    [
        ("Point1", new Coordinate(8.0804835, 63.1021952)),
        ("Point2", new Coordinate(8.0469198, 63.0922634)),
        ("Point3", new Coordinate(8.0569825, 63.0815959)),
        ("Point4", new Coordinate(8.0156699, 63.0816005)),
        ("Point5", new Coordinate(7.3819304, 62.8416539)),
        ("Point6", new Coordinate(7.3702892, 62.8377319)),
        ("Point7", new Coordinate(7.2164099, 62.8738708)),
        ("Point8", new Coordinate(7.2130345, 62.8863185)),
        ("Point9", new Coordinate(7.3334046, 62.8511178)),
        ("Point10", new Coordinate(7.3249769, 62.8545372)),
        ("Point11", new Coordinate(7.2632978, 62.8945199)),
        ("Point12", new Coordinate(7.2820371, 62.8950074)),
        ("Point13", new Coordinate(7.299503, 62.892706)),
        ("Point14", new Coordinate(7.323908, 62.892925)),
        ("Point15", new Coordinate(7.331828, 62.897589)),
        ("Point16", new Coordinate(7.3520551, 62.8502275)),
        ("Point17", new Coordinate(8.0025263, 63.1648342)),
        ("Point18", new Coordinate(8.0827132, 63.1744837)),
        ("Point19", new Coordinate(8.0720891, 63.1999423)),
        ("Point20", new Coordinate(7.5195057, 62.891951)),
        ("Point21", new Coordinate(7.4903488, 62.8540162)),
        ("Point22", new Coordinate(7.3180466, 62.956241)),
        ("Point23", new Coordinate(7.5388088, 63.029466)),
        ("Point24", new Coordinate(7.5483423, 63.0207665)),
        ("Point25", new Coordinate(7.5223752, 62.9785104)),
        ("Point26", new Coordinate(7.5047886, 62.9794194)),
        ("Point27", new Coordinate(7.4045975, 62.9788849)),
        ("Point28", new Coordinate(7.4022612, 62.9485595)),
        ("Point29", new Coordinate(7.4035293, 62.928613)),
        ("Point30", new Coordinate(8.2403845, 63.1947805)),
        ("Point31", new Coordinate(8.2307902, 63.2006741)),
        ("Point32", new Coordinate(8.2273409, 63.2022075)),
        ("Point33", new Coordinate(8.254689, 63.1746021)),
        ("Point34", new Coordinate(8.20625, 63.1916699)),
        ("Point35", new Coordinate(7.8880844, 63.0984686)),
        ("Point36", new Coordinate(7.7259933, 63.0690792)),
        ("Point37", new Coordinate(7.7320216, 63.0622538)),
        ("Point38", new Coordinate(8.0991081, 63.1713673)),
        ("Point39", new Coordinate(8.1324969, 63.1772977)),
        ("Point40", new Coordinate(8.1367444, 63.1938829)),
        ("Point41", new Coordinate(7.7378972, 63.1218614)),
        ("Point42", new Coordinate(7.7362481, 63.1307669)),
        ("Point43", new Coordinate(7.7190817, 63.1168229)),
        ("Point44", new Coordinate(7.7234442, 63.0789354)),
        ("Point45", new Coordinate(7.6438153, 63.0807811)),
        ("Point46", new Coordinate(7.6426009, 63.0131349)),
        ("Point47", new Coordinate(7.7295323, 63.0541057)),
        ("Point48", new Coordinate(7.7287855, 63.0378758)),
        ("Point49", new Coordinate(7.7103076, 63.0399492)),
        ("Point50", new Coordinate(7.6655518, 63.0638433)),
        ("Point51", new Coordinate(8.2128859, 63.1837859)),
        ("Point52", new Coordinate(8.2198458, 63.1713633)),
        ("Point53", new Coordinate(8.2038276, 63.1926328)),
        ("Point54", new Coordinate(8.1754965, 63.2336226)),
        ("Point55", new Coordinate(8.1658298, 63.2332336)),
        ("Point56", new Coordinate(8.157751, 63.2073113)),
        ("Point57", new Coordinate(7.7632266, 63.1115233)),
        ("Point58", new Coordinate(7.8081402, 63.096387)),
        ("Point59", new Coordinate(7.7649987, 63.1283514)),
        ("Point60", new Coordinate(7.8591699, 63.1211222)),
        ("Point61", new Coordinate(7.8324974, 63.1173933)),
        ("Point62", new Coordinate(7.8551953, 63.1126188)),
        ("Point63", new Coordinate(7.8465826, 63.1127022)),
        ("Point64", new Coordinate(7.7863957, 63.1316401)),
        ("Point65", new Coordinate(7.8094693, 63.1178707)),
        ("Point66", new Coordinate(7.7814327, 63.1303217))
    ];
    // 85km bike ride
    readonly static string polylineA = "owv`Ksdnp@c@~DmHzLqFrOr@xJmAnL_KhRsP|AuM_YeHzSyBzOuHdMsK|d@iEhGdPpxAvNzbArFrTzNt]rKde@eEzQiEvIfClTnHbOdKtJbGeB`Lv}@HnXfFlPbCf`@jKfj@|J~x@hk@fpAlGndAlWb_ArEpJ|C|XfOj[`Pzx@fEnFpEn@~]yI`FwLpEw`@hCmHdLmKp]aa@vFaMzJ_FrElA`fGzwOD~HzMtpAfC~rApBhYQfW`Fhf@g@lT}Epa@vBz\\e@`]Ljb@dC~n@aE`UhArUw@hVmBb@PeCYtBtAd@nCb_@Qvo@oBxKqHpHf@dMw@vRdA|_@wP]j@dJyBvCEtC}CtAyX`_@wQ|PqGh[oD`]yAzYx@pDM|RgAiBDfEcDpByBiClnCvpRxAiA}Doe@y@wv@yCca@{G{P{BaMgHsLiH]wCrCgSsa@cBGcF_OSiKdAfOh@UnC~K|A?xKzSnCrI|CpBrA_DlHZfH~LxHxV`DnOvEgIy@mUfPiD~JcOfCgy@bHgL|E_PhDyDrEj@xF~JfJlBvBlUzBxEjXnL`Fy@v\\t[fCzNrHhMpUvq@~BNhIoJ|EGvZ|Q~LxAtM|OhLbGvFvHjDzN~FjDpB`KxBiJ`DaC~BfJ|ArYMjQhAzIlLdQvAlGxFl@dE{DnMfXnIoPpHpCtBtFvDhX|JpIdGtO|Lp@pIfOjDrPpFnG~ItZjd@}CnCvHtH~j@pCjGha@oQ|DdHGbDfAM|GxWbEvFvCzKdD~b@iC`c@S`r@eJbe@qOb_BaTxu@sM`XqEv`@i@pRjBxnBvIdp@jMli@rAvOy@jzA`DpsB|c@|vB}Lvb@kM|iAYjpAgK~UkGh[wZngAaBdSdAlLdHtRyBlL~A_L`CpHrHz[jBxb@rFnj@Qzl@~FlYbBp`@rJ`i@lDxh@jHbe@p@bd@jKxb@dIr|@|FrHfN}EzFdCvCnJ~Ftd@vJ{GhDuFbKwVlLse@`a@sd@jVk]lJj@`Nx[xKbDb[eIvKwM|LaYxGcWhC_z@cCyyBzDaw@tB{H`HcIrG}MlGsChLjAn@cExBIxCwL`C_s@rDm^c@sc@jJcdBbMujArFyNxHwGbJaa@zJcV`bAubB|G}PpO}JfLsAxJkKnU{A|N_StW`K`GjHtSbh@p[frAzT|ZvOrJbJzJbZrkAlDrh@Sjl@hAld@rIffA|YjoBnU|gAzi@|lDfQ~l@lPfcBhU~|AnJj\\sBs@iHdS_Gm@UwJ_Dv@yB|DlGkFh@tJ|CK";


    // The 100 peaks that SummitsWorker will fetch for this activity
    readonly static List<(string, Coordinate)> pointsB =
    [
        ("Point1", new Coordinate(15.4613636, 60.6586723)),
        ("Point2", new Coordinate(15.4322384, 60.6642815)),
        ("Point3", new Coordinate(15.4158445, 60.6692318)),
        ("Point4", new Coordinate(15.3893467, 60.6525879)),
        ("Point5", new Coordinate(15.6037057, 60.7454625)),
        ("Point6", new Coordinate(15.5926784, 60.742288)),
        ("Point7", new Coordinate(15.6457751, 60.5971075)),
        ("Point8", new Coordinate(15.5820978, 60.7508414)),
        ("Point9", new Coordinate(15.7177495, 60.6354091)),
        ("Point10", new Coordinate(15.5568697, 60.7413543)),
        ("Point11", new Coordinate(16.4226683, 60.6505342)),
        ("Point12", new Coordinate(16.4075235, 60.6525513)),
        ("Point13", new Coordinate(16.4985471, 60.6163738)),
        ("Point14", new Coordinate(16.2592444, 60.6643327)),
        ("Point15", new Coordinate(16.5235042, 60.6436201)),
        ("Point16", new Coordinate(16.5020751, 60.5352889)),
        ("Point17", new Coordinate(15.4866238, 60.6054855)),
        ("Point18", new Coordinate(16.1311115, 60.6728081)),
        ("Point19", new Coordinate(15.9922732, 60.64499)),
        ("Point20", new Coordinate(16.0357786, 60.6150662)),
        ("Point21", new Coordinate(16.0609699, 60.6143081)),
        ("Point22", new Coordinate(16.0073488, 60.6484704)),
        ("Point23", new Coordinate(16.0375046, 60.6341428)),
        ("Point24", new Coordinate(15.3049528, 60.6762392)),
        ("Point25", new Coordinate(16.3834786, 60.5423638)),
        ("Point26", new Coordinate(15.2930045, 60.5427839)),
        ("Point27", new Coordinate(15.9721918, 60.6605112)),
        ("Point28", new Coordinate(16.0929861, 60.634628)),
        ("Point29", new Coordinate(15.4706593, 60.613973)),
        ("Point30", new Coordinate(16.4051508, 60.5516064)),
        ("Point31", new Coordinate(16.3888859, 60.5251186)),
        ("Point32", new Coordinate(16.0025137, 60.6599806)),
        ("Point33", new Coordinate(15.6340268, 60.5605368)),
        ("Point34", new Coordinate(15.5329661, 60.5749092)),
        ("Point35", new Coordinate(16.2872243, 60.6496307)),
        ("Point36", new Coordinate(16.238215, 60.6220211)),
        ("Point37", new Coordinate(16.3080402, 60.5903138)),
        ("Point38", new Coordinate(15.4359539, 60.5785773)),
        ("Point39", new Coordinate(15.435493, 60.7569067)),
        ("Point40", new Coordinate(15.3068671, 60.6862098)),
        ("Point41", new Coordinate(15.4258394, 60.5718769)),
        ("Point42", new Coordinate(15.4675063, 60.5320712)),
        ("Point43", new Coordinate(15.9484832, 60.6602329)),
        ("Point44", new Coordinate(15.4795558, 60.5138515)),
        ("Point45", new Coordinate(16.0496132, 60.6492545)),
        ("Point46", new Coordinate(15.5052158, 60.6338017)),
        ("Point47", new Coordinate(16.1968446, 60.6273681)),
        ("Point48", new Coordinate(16.214451, 60.6698564)),
        ("Point49", new Coordinate(16.5146965, 60.5245223)),
        ("Point50", new Coordinate(16.0217508, 60.6685795)),
        ("Point51", new Coordinate(15.4465447, 60.7542753)),
        ("Point52", new Coordinate(15.5290595, 60.5769699)),
        ("Point53", new Coordinate(15.6124429, 60.5697449)),
        ("Point54", new Coordinate(15.4354901, 60.5481835)),
        ("Point55", new Coordinate(15.4926963, 60.6633808)),
        ("Point56", new Coordinate(16.0517525, 60.6297862)),
        ("Point57", new Coordinate(16.4728391, 60.5280687)),
        ("Point58", new Coordinate(15.2943881, 60.7081587)),
        ("Point59", new Coordinate(15.4629789, 60.5437324)),
        ("Point60", new Coordinate(16.2455192, 60.638394)),
        ("Point61", new Coordinate(16.418895, 60.5361396)),
        ("Point62", new Coordinate(15.4503782, 60.7430042)),
        ("Point63", new Coordinate(15.422939, 60.7358255)),
        ("Point64", new Coordinate(15.4526286, 60.5242265)),
        ("Point65", new Coordinate(15.3620401, 60.5777659)),
        ("Point66", new Coordinate(15.9894622, 60.6463474)),
        ("Point67", new Coordinate(15.9587563, 60.6429708)),
        ("Point68", new Coordinate(16.2493816, 60.6290899)),
        ("Point69", new Coordinate(15.635757, 60.6323634)),
        ("Point70", new Coordinate(16.3740466, 60.5823485)),
        ("Point71", new Coordinate(15.3439942, 60.5726109)),
        ("Point72", new Coordinate(15.5742509, 60.6297311)),
        ("Point73", new Coordinate(15.5643531, 60.6312446)),
        ("Point74", new Coordinate(15.5469939, 60.6246142)),
        ("Point75", new Coordinate(15.6352218, 60.5874854)),
        ("Point76", new Coordinate(15.5984004, 60.5873442)),
        ("Point77", new Coordinate(15.5301711, 60.6211616)),
        ("Point78", new Coordinate(15.5239391, 60.6053508)),
        ("Point79", new Coordinate(15.5049117, 60.6519373)),
        ("Point80", new Coordinate(15.3384099, 60.6839615)),
        ("Point81", new Coordinate(15.3776989, 60.6965875)),
        ("Point82", new Coordinate(15.3660259, 60.7074974)),
        ("Point83", new Coordinate(15.3601303, 60.5686678)),
        ("Point84", new Coordinate(15.3277836, 60.545577)),
        ("Point85", new Coordinate(15.9492076, 60.6462212)),
        ("Point86", new Coordinate(15.9839108, 60.6024008)),
        ("Point87", new Coordinate(15.3936016, 60.7530181)),
        ("Point88", new Coordinate(15.9755423, 60.6016635)),
        ("Point89", new Coordinate(15.9179685, 60.6467676)),
        ("Point90", new Coordinate(15.8511748, 60.6683305)),
        ("Point91", new Coordinate(15.8284061, 60.654965)),
        ("Point92", new Coordinate(15.6138566, 60.5616547)),
        ("Point93", new Coordinate(15.6034799, 60.5388611)),
        ("Point94", new Coordinate(15.9353446, 60.612933)),
        ("Point95", new Coordinate(15.3739034, 60.7397849)),
        ("Point96", new Coordinate(15.3612005, 60.7565928)),
        ("Point97", new Coordinate(15.3076396, 60.5868713)),
        ("Point98", new Coordinate(15.2789084, 60.6343511)),
        ("Point99", new Coordinate(15.4131578, 60.6856342)),
        ("Point100", new Coordinate(15.5616287, 60.7539342))
    ];
    // 147km A-B bike ride
    readonly static string polylineB = "gg{pJqfdfBfKkVsn@ko@wNxClA~l@qPLq@fPgFzIaAdjAsi@bw@cb@toBkPhgBoHrxA~VdjCrMxhBB|p@nGlMuFbDpO`lEkErdBoGhUClh@fQbw@~AfvBr[`r@`NF|J`}@lXp{@|BziBmBl^hMd`DcHjp@sBjx@fHzq@tRjFmIfh@dW~eCfu@p|C`N~F`Xxf@~a@hXbG`LmCeGiDf^}HtOyI~xBoKh]uFve@aWrj@`Gdv@Lhv@wErm@uNdVqMfn@w\\zbA{LxwBia@t{AdDd`A{E`XeRx`@_Fd_A~Dfo@sMjq@gGd{AmNxs@u[vv@gJb{@qVxn@cCt_@hBjcAaBpg@}Tf~BoOrmAyUh{@l@ro@aKlqApKzmCpM|qCiDpkBwJpt@fHvM`P{RdMzm@?`ZpKdr@|YfjAgMreAPt^yMvr@[rn@sFbPc\\~^uUu_@}c@yL|Elf@{Fb}@lDnoB{Db`ArBrHfKnC`DzNdGnnAUhmBbRbpDaNnu@eShwBdCt[iDl\\tIzvBo@js@dHxg@mBdsBnNn_AvEdy@_Ght@mRdt@|p@v]Zti@|NrNjLrn@fZpk@bK|n@pLddBnFlSzBeBqDdQ|Yp}@njA`_Avs@bZkC`GbElXnFtBzLhm@nI~oAKjzArBxnBfE`JkSn{@eP`tA_b@p`BmAtZlOIjDtN}GhYnJiE|Cb]rDiEtJxXwFcAqSj_AmH~KwSlCaVxZkGmKrFdKbMbs@fE}@eEfB~BpsBhJxpAjBrc@eAzz@fFtl@qFp|AjSjlBzBto@tJbx@`Xpd@fa@va@nEtWlCtgAbMxb@fOhtAlMpUfS~x@~c@fe@~c@~yBvt@fiAbWxMp}@gYbFl^hl@}]|P`FNnk@dc@qNhBjYzWfhAfCn^pa@zfA}pAdiDm`@pT_Ocj@cJsr@m[gCkVaVqb@fFeJc^qBacBbB_e@vO_aAqGsn@k[gVmt@miAwb@axBqc@ee@oLad@mGlCsDtQkQhLkPtp@}Bne@{Rpv@crA~s@kReD_YtJw[d^gNe_@i@eWkYoQe\\_KmDkSs^ku@sBmh@eEaEqm@gRyLlCaOz^mMxM}Ul`A{LaCgIiZcOqKag@pDmd@qTwR}`@sMuQ{GylBmHqa@oWwN_FkR_OkSuJ~WoF}Bf@cF_KOeFdRiHxDkk@tDae@|oAmkApr@sZno@aZuQmFmh@nMgNjP_b@tEeW~BeeAlZ_}@xBd@";


    readonly List<(string, Coordinate)> points = pointsB;
    readonly string polyline = polylineB;

    ISummitsCalculator _baseline = new BasicSummitsCalculator();
    ISummitsCalculator _summitsCalculatorSimpleFilter = new SummitsCalculatorWithSimpleFilter(85000);
    ISummitsCalculator _boundingBox = new SummitsCalculatorWithBoundingBoxFilter();

    public SummitsBenchmarks()
    {

    }

    [Benchmark(Baseline = true)]
    public string[] CompareAllPoints() => _baseline.FindPointsNearRoute(points, polyline).ToArray();

    [Benchmark]
    public string[] ActivityLengthFilter() => _summitsCalculatorSimpleFilter.FindPointsNearRoute(points, polyline).ToArray();

    [Benchmark]
    public string[] BoundingBoxFilter() => _boundingBox.FindPointsNearRoute(points, polyline).ToArray();
}

public class Program
{
    public static void Main(string[] args)
    {
        BenchmarkRunner.Run<SummitsBenchmarks>();
    }
}
