use proc_macro::{self, TokenStream};
use quote::quote;
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::DeriveInput;
use syn::LitStr;

#[proc_macro_derive(FlattenStructFileds)]
pub fn derive_flatten_struct_fileds(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = ast.ident;

    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(fields),
        ..
    }) = ast.data
    {
        fields
    } else {
        panic!("Only Support Struct")
    };

    let mut names = vec![];
    let mut idents = vec![];

    for field in fields.named.iter() {
        let name = field.ident.as_ref().unwrap().to_string();
        let literal_key_str = LitStr::new(&name, field.span());

        names.push(quote! { #literal_key_str });
        idents.push(field.ident.as_ref().unwrap());
    }

    let expended = quote! {
        impl #struct_name {
            fn fields_name(&self) -> Vec<String> {
                let mut names = vec![];
                #(
                    names.push(#names.to_string());
                )*

                names
            }

            fn fields_value(&self) -> Vec<String> {
                let mut values = vec![];
                #(
                    values.push(foramt!("{}", self.#idents));
                )*

                values
            }
        }
    };

    expended.into()
}
