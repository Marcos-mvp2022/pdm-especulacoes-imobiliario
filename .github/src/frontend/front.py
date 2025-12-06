import streamlit as st
import requests

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="Predição Imobiliária", page_icon="🏢", layout="centered")

# --- SUA URL DA API (Já atualizada) ---
API_URL = "https://api-imoveis-pdm-886529222714.us-central1.run.app"

st.title("🏢 Inteligência Imobiliária")
st.markdown("### Descubra o valor de mercado do seu imóvel")
st.divider()

# --- INPUTS ---
with st.sidebar:
    st.header("Características")
    tipo = st.selectbox("Tipo", ["Apartamento", "Casa", "Conjunto/Comercial"])
    area = st.number_input("Área Útil (m²)", 20, 2000, 100)
    quartos = st.slider("Quartos", 1, 6, 3)
    suites = st.slider("Suítes", 0, 5, 1)
    banheiros = st.slider("Banheiros", 1, 7, 2)
    vagas = st.slider("Vagas", 0, 6, 1)

    st.markdown("---")
    tem_piscina = st.checkbox("🏊 Tem Piscina?")
    tem_academia = st.checkbox("🏋️ Tem Academia?")
    tem_elevador = st.checkbox("🛗 Tem Elevador?")

    mapa_tipo = {"Apartamento": "APARTMENT", "Casa": "HOME", "Conjunto/Comercial": "UNIT"}

# --- AÇÃO ---
if st.button("💰 Calcular Preço", type="primary", use_container_width=True):

    # O Pacote JSON exato que a API V2 espera
    payload = {
        "usable_area_m2": area,
        "bedrooms": quartos,
        "bathrooms": banheiros,
        "suites": suites,
        "parking_spaces": vagas,
        "has_pool": tem_piscina,
        "has_gym": tem_academia,
        "has_elevator": tem_elevador,
        "property_type_slug": mapa_tipo[tipo]
    }

    with st.spinner('Consultando a IA no Google Cloud...'):
        try:
            resp = requests.post(f"{API_URL}/predict", json=payload)
            if resp.status_code == 200:
                valor = resp.json()['preco_previsto']
                st.success(f"Avaliação: **R$ {valor:,.2f}**")
                # Mostra o JSON bruto pro professor ver que é real
                with st.expander("Ver dados técnicos (JSON)"):
                    st.json(resp.json())
            else:
                st.error("Erro na API")
                st.write(resp.text)
        except Exception as e:
            st.error(f"Erro de conexão: {e}")
