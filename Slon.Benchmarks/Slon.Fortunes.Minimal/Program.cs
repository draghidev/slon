using System.Text.Encodings.Web;
using System.Text.Unicode;
using Slon.Fortunes.Minimal;
using Slon.Fortunes.Minimal.Templates;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();

await using var database = await FortuneDatabase.CreateAsync(builder.Configuration);
builder.Services.AddSingleton(CreateHtmlEncoder());

await using var app = builder.Build();

app.MapGet(
    "/fortunes",
    async (HtmlEncoder htmlEncoder, CancellationToken cancellationToken) =>
    {
        var template = Fortunes.Create(await database.LoadAsync(cancellationToken));
        template.HtmlEncoder = htmlEncoder;
        return template;
    });

app.Lifetime.ApplicationStarted.Register(static () => Console.WriteLine("Application started."));

await app.RunAsync();

static HtmlEncoder CreateHtmlEncoder()
{
    var settings = new TextEncoderSettings(
        UnicodeRanges.BasicLatin,
        UnicodeRanges.Katakana,
        UnicodeRanges.Hiragana);
    settings.AllowCharacter('\u2014');
    return HtmlEncoder.Create(settings);
}
