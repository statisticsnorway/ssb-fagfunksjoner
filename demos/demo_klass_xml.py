from fagfunksjoner.data.klass_xml import make_klass_xml_codelist


# ### Using the main parameters

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
    names_engelsk=names_engelsk,
)

print("KLASS XML generated and saved to:", xml_output_path)
print(df.head())

# ### Using all the parameters

# +
# Define data
codes = ["100", "110", "120"]
parent = [None, "100", "100"]

names_bokmaal = ["Hovedområde", "Underområde A", "Underområde B"]
names_nynorsk = ["Hovudområde", "Underområde A", "Underområde B"]
names_engelsk = ["Main Area", "Subarea A", "Subarea B"]

shortname_bokmaal = ["HO", "UA", "UB"]
shortname_nynorsk = ["HO", "UA", "UB"]
shortname_engelsk = ["MA", "SA", "SB"]

notes_bokmaal = ["Overordnet kategori", "Del av hovedområdet", "Del av hovedområdet"]
notes_nynorsk = ["Overordna kategori", "Del av hovudområdet", "Del av hovudområdet"]
notes_engelsk = ["Top-level category", "Part of main area", "Part of main area"]

valid_from = ["2025-01-01", "2025-01-01", "2025-01-01"]
valid_to = ["2030-12-31", "2030-12-31", "2030-12-31"]

# Define output path
xml_path = "full_klass_codelist.xml"

# Create XML and DataFrame
df = make_klass_xml_codelist(
    path=xml_path,
    codes=codes,
    names_bokmaal=names_bokmaal,
    names_nynorsk=names_nynorsk,
    names_engelsk=names_engelsk,
    parent=parent,
    shortname_bokmaal=shortname_bokmaal,
    shortname_nynorsk=shortname_nynorsk,
    shortname_engelsk=shortname_engelsk,
    notes_bokmaal=notes_bokmaal,
    notes_nynorsk=notes_nynorsk,
    notes_engelsk=notes_engelsk,
    valid_from=valid_from,
    valid_to=valid_to,
)

print("✅ KLASS XML created at:", xml_path)
print(df.head())
# -
