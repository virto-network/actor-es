extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(EntityName)]
pub fn entity_name_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_name_macro(&ast)
}

fn impl_name_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl EntityName for #name {
            const NAME: &'static str = stringify!(#name);
        }
    };
    gen.into()
}
