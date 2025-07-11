from fagfunksjoner.data.klass_xml import make_klass_xml_codelist

# Define your classification codes and names
codes = ["01", "02", "03"]
names_bokmaal = ["Oslo", "Bergen", "Trondheim"]

# Optional: include Nynorsk and English names
names_nynorsk = ["Oslo", "Bergen", "Trondheim"]
names_engelsk = ["Oslo", "Bergen", "Trondheim"]

# Output XML path
xml_output_path = "kommune_koder.xml"

# Generate and save the XML file
df = make_klass_xml_codelist(
    path=xml_output_path,
    codes=codes,
    names_bokmaal=names_bokmaal,
    names_nynorsk=names_nynorsk,
    names_engelsk=names_engelsk
)

print("KLASS XML generated and saved to:", xml_output_path)
print(df.head())